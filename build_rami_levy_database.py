#!/usr/bin/env python3
"""
Create/refresh a local SQLite database with Rami Levy product data.

Data source:
  https://url.publishedprices.co.il (Cerberus portal, username-based login)

This script:
  1) Logs in to the portal as RamiLevi.
  2) Lists PriceFull files for chain 7290058140886.
  3) Selects the newest PriceFull file per branch.
  4) Downloads and parses product XML.
  5) Upserts products and branch prices into SQLite.
"""

from __future__ import annotations

import argparse
import gzip
import io
import re
import sqlite3
import sys
import urllib.parse
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests
import urllib3


DEFAULT_BASE_URL = "https://url.publishedprices.co.il"
DEFAULT_USERNAME = "RamiLevi"
DEFAULT_CHAIN_ID = "7290058140886"
DEFAULT_TIMEOUT = 45

CSRF_META_RE = re.compile(r'<meta name="csrftoken" content="([^"]+)"', re.IGNORECASE)
PRICEFULL_RE = re.compile(r"^PriceFull(?P<chain>\d{13})-(?P<rest>.+)\.gz$", re.IGNORECASE)


@dataclass(frozen=True)
class PriceFullFile:
    file_name: str
    branch_id: str
    timestamp_raw: str
    timestamp: Optional[datetime]


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a SQLite database with all Rami Levy products."
    )
    parser.add_argument(
        "--db-path",
        default="rami_levy_products.db",
        help="SQLite output path (default: rami_levy_products.db)",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Cerberus base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--username",
        default=DEFAULT_USERNAME,
        help=f"Portal username (default: {DEFAULT_USERNAME})",
    )
    parser.add_argument(
        "--chain-id",
        default=DEFAULT_CHAIN_ID,
        help=f"Rami Levy chain id (default: {DEFAULT_CHAIN_ID})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Rows to fetch per /file/json/dir call (default: 200)",
    )
    parser.add_argument(
        "--max-branches",
        type=int,
        default=None,
        help="Optional safety limit: ingest only N branches",
    )
    parser.add_argument(
        "--verify-ssl",
        action="store_true",
        help="Enable certificate verification (off by default due CA chain issues on this host).",
    )
    return parser.parse_args()


def extract_csrf(html: str) -> str:
    match = CSRF_META_RE.search(html)
    if not match:
        raise RuntimeError("Could not find csrftoken in page HTML.")
    return match.group(1)


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text.replace(",", "."))
    except ValueError:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def parse_timestamp(ts_raw: str) -> Optional[datetime]:
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M"):
        try:
            return datetime.strptime(ts_raw, fmt)
        except ValueError:
            pass
    return None


def parse_pricefull_filename(file_name: str, chain_id: str) -> Optional[PriceFullFile]:
    if file_name.startswith("NULL"):
        return None

    match = PRICEFULL_RE.match(file_name)
    if not match:
        return None
    if match.group("chain") != chain_id:
        return None

    rest = match.group("rest")
    parts = rest.split("-")
    if len(parts) < 2:
        return None

    branch_id = parts[0]
    if not branch_id.isdigit():
        return None

    if len(parts) >= 3 and parts[-2].isdigit() and len(parts[-2]) == 8 and parts[-1].isdigit():
        timestamp_raw = parts[-2] + parts[-1]
    else:
        timestamp_raw = parts[-1]

    return PriceFullFile(
        file_name=file_name,
        branch_id=branch_id,
        timestamp_raw=timestamp_raw,
        timestamp=parse_timestamp(timestamp_raw),
    )


def newer_than(left: PriceFullFile, right: PriceFullFile) -> bool:
    left_key = (left.timestamp or datetime.min, left.timestamp_raw, left.file_name)
    right_key = (right.timestamp or datetime.min, right.timestamp_raw, right.file_name)
    return left_key > right_key


def pick_latest_per_branch(files: Sequence[PriceFullFile]) -> List[PriceFullFile]:
    latest: Dict[str, PriceFullFile] = {}
    for file_entry in files:
        current = latest.get(file_entry.branch_id)
        if current is None or newer_than(file_entry, current):
            latest[file_entry.branch_id] = file_entry
    return sorted(latest.values(), key=lambda x: x.branch_id)


class PublishedPricesClient:
    def __init__(self, base_url: str, timeout: int, verify_ssl: bool):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.file_csrf_token: Optional[str] = None

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def login(self, username: str) -> None:
        login_page = self.session.get(
            self._url("/login"),
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        login_page.raise_for_status()
        login_csrf = extract_csrf(login_page.text)

        response = self.session.post(
            self._url("/login/user"),
            data={
                "username": username,
                "password": "",
                "r": "",
                "csrftoken": login_csrf,
            },
            headers={
                "Referer": self._url("/login"),
                "Origin": self.base_url,
            },
            timeout=self.timeout,
            verify=self.verify_ssl,
            allow_redirects=True,
        )
        response.raise_for_status()
        if "/file" not in response.url:
            raise RuntimeError(
                "Login did not reach /file. Username may be invalid or session is blocked."
            )

        self.refresh_file_csrf()

    def refresh_file_csrf(self) -> None:
        file_page = self.session.get(
            self._url("/file"),
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        file_page.raise_for_status()
        self.file_csrf_token = extract_csrf(file_page.text)

    def _datatable_payload(self, start: int, length: int, search: str) -> Dict[str, str]:
        if self.file_csrf_token is None:
            raise RuntimeError("Not authenticated: missing file CSRF token.")

        return {
            "sEcho": "1",
            "iColumns": "4",
            "sColumns": "",
            "iDisplayStart": str(start),
            "iDisplayLength": str(length),
            "mDataProp_0": "Name",
            "mDataProp_1": "Type",
            "mDataProp_2": "Size",
            "mDataProp_3": "Date",
            "sSearch": search,
            "bRegex": "false",
            "sSearch_0": "",
            "bRegex_0": "false",
            "bSearchable_0": "true",
            "bSortable_0": "true",
            "sSearch_1": "",
            "bRegex_1": "false",
            "bSearchable_1": "true",
            "bSortable_1": "true",
            "sSearch_2": "",
            "bRegex_2": "false",
            "bSearchable_2": "true",
            "bSortable_2": "true",
            "sSearch_3": "",
            "bRegex_3": "false",
            "bSearchable_3": "true",
            "bSortable_3": "true",
            "iSortCol_0": "0",
            "sSortDir_0": "desc",
            "iSortingCols": "1",
            "cd": "/",
            "csrftoken": self.file_csrf_token,
        }

    def list_directory_files(self, search: str, page_size: int) -> List[str]:
        files: List[str] = []
        start = 0
        total = None

        while True:
            payload = self._datatable_payload(start=start, length=page_size, search=search)
            response = self.session.post(
                self._url("/file/json/dir"),
                data=payload,
                headers={
                    "Referer": self._url("/file"),
                    "Origin": self.base_url,
                },
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            data = response.json()

            # CSRF token on /file can rotate; refresh once and retry the page.
            if data.get("error") == "CSRF security check failed":
                self.refresh_file_csrf()
                payload = self._datatable_payload(start=start, length=page_size, search=search)
                response = self.session.post(
                    self._url("/file/json/dir"),
                    data=payload,
                    headers={
                        "Referer": self._url("/file"),
                        "Origin": self.base_url,
                    },
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )
                response.raise_for_status()
                data = response.json()

            if data.get("error"):
                raise RuntimeError(f"File listing API returned error: {data['error']}")

            rows = data.get("aaData", [])
            for row in rows:
                value = row.get("value")
                if value:
                    files.append(value)

            if total is None:
                total = int(data.get("iTotalDisplayRecords", data.get("iTotalRecords", 0)))
            start += page_size

            if not rows or start >= total:
                break

        # Keep order but remove duplicates.
        dedup: List[str] = []
        seen = set()
        for file_name in files:
            if file_name in seen:
                continue
            seen.add(file_name)
            dedup.append(file_name)
        return dedup

    def download_file(self, file_name: str) -> bytes:
        encoded_name = urllib.parse.quote(file_name, safe="")
        response = self.session.get(
            self._url(f"/file/d/{encoded_name}"),
            headers={"Referer": self._url("/file")},
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        response.raise_for_status()
        return response.content


def decode_archive(payload: bytes) -> bytes:
    if payload.startswith(b"PK\x03\x04"):
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            members = zf.namelist()
            if not members:
                raise RuntimeError("Zip archive had no files.")
            return zf.read(members[0])
    if payload.startswith(b"\x1f\x8b"):
        return gzip.decompress(payload)
    return payload


def strip_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def iter_items(xml_bytes: bytes) -> Iterable[Dict[str, str]]:
    context = ET.iterparse(io.BytesIO(xml_bytes), events=("end",))
    for _, elem in context:
        if strip_namespace(elem.tag).lower() != "item":
            continue
        record: Dict[str, str] = {}
        for child in list(elem):
            key = strip_namespace(child.tag)
            record[key] = (child.text or "").strip()
        yield record
        elem.clear()


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    chain_id TEXT NOT NULL,
    files_discovered INTEGER DEFAULT 0,
    files_processed INTEGER DEFAULT 0,
    products_upserted INTEGER DEFAULT 0,
    branch_price_rows INTEGER DEFAULT 0,
    status TEXT NOT NULL,
    error TEXT
);

CREATE TABLE IF NOT EXISTS branches (
    branch_id TEXT PRIMARY KEY,
    chain_id TEXT NOT NULL,
    latest_file_name TEXT NOT NULL,
    latest_file_timestamp TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    item_code TEXT PRIMARY KEY,
    item_name TEXT,
    manufacturer_name TEXT,
    manufacturer_item_description TEXT,
    manufacture_country TEXT,
    item_type INTEGER,
    unit_qty TEXT,
    quantity REAL,
    unit_of_measure TEXT,
    is_weighted INTEGER,
    allow_discount INTEGER,
    last_sale_datetime TEXT,
    last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS branch_product_prices (
    branch_id TEXT NOT NULL,
    item_code TEXT NOT NULL,
    item_price REAL,
    unit_of_measure_price REAL,
    qty_in_package REAL,
    price_update_time TEXT,
    item_status INTEGER,
    source_file_name TEXT NOT NULL,
    source_file_timestamp TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (branch_id, item_code),
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id),
    FOREIGN KEY (item_code) REFERENCES products(item_code)
);

CREATE INDEX IF NOT EXISTS idx_products_name ON products(item_name);
CREATE INDEX IF NOT EXISTS idx_branch_prices_item_code ON branch_product_prices(item_code);
"""


PRODUCT_UPSERT_SQL = """
INSERT INTO products (
    item_code,
    item_name,
    manufacturer_name,
    manufacturer_item_description,
    manufacture_country,
    item_type,
    unit_qty,
    quantity,
    unit_of_measure,
    is_weighted,
    allow_discount,
    last_sale_datetime,
    last_seen_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(item_code) DO UPDATE SET
    item_name = excluded.item_name,
    manufacturer_name = excluded.manufacturer_name,
    manufacturer_item_description = excluded.manufacturer_item_description,
    manufacture_country = excluded.manufacture_country,
    item_type = excluded.item_type,
    unit_qty = excluded.unit_qty,
    quantity = excluded.quantity,
    unit_of_measure = excluded.unit_of_measure,
    is_weighted = excluded.is_weighted,
    allow_discount = excluded.allow_discount,
    last_sale_datetime = excluded.last_sale_datetime,
    last_seen_at = excluded.last_seen_at
"""


BRANCH_PRICE_UPSERT_SQL = """
INSERT INTO branch_product_prices (
    branch_id,
    item_code,
    item_price,
    unit_of_measure_price,
    qty_in_package,
    price_update_time,
    item_status,
    source_file_name,
    source_file_timestamp,
    updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(branch_id, item_code) DO UPDATE SET
    item_price = excluded.item_price,
    unit_of_measure_price = excluded.unit_of_measure_price,
    qty_in_package = excluded.qty_in_package,
    price_update_time = excluded.price_update_time,
    item_status = excluded.item_status,
    source_file_name = excluded.source_file_name,
    source_file_timestamp = excluded.source_file_timestamp,
    updated_at = excluded.updated_at
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def begin_run(conn: sqlite3.Connection, chain_id: str) -> int:
    started_at = now_utc_iso()
    cursor = conn.execute(
        """
        INSERT INTO ingestion_runs (started_at, chain_id, status)
        VALUES (?, ?, 'running')
        """,
        (started_at, chain_id),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    files_discovered: int,
    files_processed: int,
    products_upserted: int,
    branch_price_rows: int,
    error: Optional[str] = None,
) -> None:
    conn.execute(
        """
        UPDATE ingestion_runs
        SET completed_at = ?,
            status = ?,
            files_discovered = ?,
            files_processed = ?,
            products_upserted = ?,
            branch_price_rows = ?,
            error = ?
        WHERE id = ?
        """,
        (
            now_utc_iso(),
            status,
            files_discovered,
            files_processed,
            products_upserted,
            branch_price_rows,
            error,
            run_id,
        ),
    )
    conn.commit()


def ingest_branch_file(
    conn: sqlite3.Connection,
    branch_id: str,
    file_name: str,
    file_timestamp_raw: str,
    xml_bytes: bytes,
) -> Tuple[int, int]:
    now_iso = now_utc_iso()
    products_batch: List[Tuple[object, ...]] = []
    prices_batch: List[Tuple[object, ...]] = []
    rows_count = 0

    for record in iter_items(xml_bytes):
        record_lc = {k.lower(): v for k, v in record.items()}
        item_code = record_lc.get("itemcode")
        if not item_code:
            continue

        products_batch.append(
            (
                item_code,
                record_lc.get("itemname"),
                record_lc.get("manufacturename"),
                record_lc.get("manufactureitemdescription"),
                record_lc.get("manufacturecountry"),
                parse_int(record_lc.get("itemtype")),
                record_lc.get("unitqty"),
                parse_float(record_lc.get("quantity")),
                record_lc.get("unitofmeasure"),
                parse_int(record_lc.get("bisweighted")),
                parse_int(record_lc.get("allowdiscount")),
                record_lc.get("lastsaledatetime"),
                now_iso,
            )
        )
        prices_batch.append(
            (
                branch_id,
                item_code,
                parse_float(record_lc.get("itemprice")),
                parse_float(record_lc.get("unitofmeasureprice")),
                parse_float(record_lc.get("qtyinpackage")),
                record_lc.get("priceupdatetime"),
                parse_int(record_lc.get("itemstatus")),
                file_name,
                file_timestamp_raw,
                now_iso,
            )
        )
        rows_count += 1

        if len(products_batch) >= 1000:
            conn.executemany(PRODUCT_UPSERT_SQL, products_batch)
            conn.executemany(BRANCH_PRICE_UPSERT_SQL, prices_batch)
            products_batch.clear()
            prices_batch.clear()

    if products_batch:
        conn.executemany(PRODUCT_UPSERT_SQL, products_batch)
        conn.executemany(BRANCH_PRICE_UPSERT_SQL, prices_batch)

    conn.commit()
    return rows_count, rows_count


def main() -> int:
    args = parse_args()
    if not args.verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    conn = sqlite3.connect(args.db_path)
    try:
        ensure_schema(conn)
        run_id = begin_run(conn, args.chain_id)

        files_processed = 0
        products_upserted = 0
        branch_price_rows = 0
        files_discovered = 0

        try:
            client = PublishedPricesClient(
                base_url=args.base_url,
                timeout=args.timeout,
                verify_ssl=args.verify_ssl,
            )
            print("Logging in to published prices portal...")
            client.login(args.username)

            search_term = f"PriceFull{args.chain_id}"
            print(f"Listing files for {search_term}...")
            raw_files = client.list_directory_files(search=search_term, page_size=args.page_size)
            parsed_files = [
                pf
                for pf in (
                    parse_pricefull_filename(file_name=f, chain_id=args.chain_id) for f in raw_files
                )
                if pf is not None
            ]

            latest_per_branch = pick_latest_per_branch(parsed_files)
            if args.max_branches is not None:
                latest_per_branch = latest_per_branch[: args.max_branches]

            files_discovered = len(latest_per_branch)
            print(f"Found {len(parsed_files)} PriceFull files, using {files_discovered} newest branch snapshots.")

            for index, file_entry in enumerate(latest_per_branch, start=1):
                print(
                    f"[{index}/{files_discovered}] Branch {file_entry.branch_id}: "
                    f"{file_entry.file_name}"
                )

                conn.execute(
                    """
                    INSERT INTO branches (branch_id, chain_id, latest_file_name, latest_file_timestamp, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(branch_id) DO UPDATE SET
                        chain_id = excluded.chain_id,
                        latest_file_name = excluded.latest_file_name,
                        latest_file_timestamp = excluded.latest_file_timestamp,
                        updated_at = excluded.updated_at
                    """,
                    (
                        file_entry.branch_id,
                        args.chain_id,
                        file_entry.file_name,
                        file_entry.timestamp_raw,
                        now_utc_iso(),
                    ),
                )
                conn.commit()

                archive_bytes = client.download_file(file_entry.file_name)
                xml_bytes = decode_archive(archive_bytes)
                p_count, bp_count = ingest_branch_file(
                    conn=conn,
                    branch_id=file_entry.branch_id,
                    file_name=file_entry.file_name,
                    file_timestamp_raw=file_entry.timestamp_raw,
                    xml_bytes=xml_bytes,
                )
                files_processed += 1
                products_upserted += p_count
                branch_price_rows += bp_count
                print(f"    upserted {p_count} products/prices")

            finish_run(
                conn=conn,
                run_id=run_id,
                status="completed",
                files_discovered=files_discovered,
                files_processed=files_processed,
                products_upserted=products_upserted,
                branch_price_rows=branch_price_rows,
            )
            print(
                "Done. "
                f"branches={files_processed}, "
                f"product_rows={products_upserted}, "
                f"db={args.db_path}"
            )
        except Exception as exc:
            finish_run(
                conn=conn,
                run_id=run_id,
                status="failed",
                files_discovered=files_discovered,
                files_processed=files_processed,
                products_upserted=products_upserted,
                branch_price_rows=branch_price_rows,
                error=str(exc),
            )
            raise
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        raise SystemExit(130)
