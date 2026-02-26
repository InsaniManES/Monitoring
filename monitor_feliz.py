#!/usr/bin/env python3
"""
Monitor https://www.feliz.co.il/event/congress-{year}/
Year is taken from the current run time (Israel timezone).
Alert when the page is no longer password-protected (registration/open).
Runs on an interval; times shown in Israel time.
"""
import os
import time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Load .env from the project root so it works when run from WebStorm/IDE
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

BLOCK_TEXT = "This content is password-protected. To view it, please enter the password below"
INTERVAL = int(os.environ.get("INTERVAL_SECONDS", "60"))
ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")
# Target from .env: "20.2.2026 12:00" (day.month.year hour:minute, Israel time). 15 min before → poll every 1 sec.
TARGET_DATETIME_STR = os.environ.get("TARGET_DATETIME", "").strip()


def _israel_time_str(dt: datetime) -> str:
    """Format as Israeli date/time e.g. 20.2.2026 12:00"""
    return f"{dt.day}.{dt.month}.{dt.year} {dt.hour:02d}:{dt.minute:02d}"


def _israel_date_str(dt: datetime) -> str:
    """Date only e.g. 20.2.2026"""
    return f"{dt.day}.{dt.month}.{dt.year}"


def _israel_time_only_str(dt: datetime) -> str:
    """Time only e.g. 12:00"""
    return f"{dt.hour:02d}:{dt.minute:02d}"


def _target_datetime() -> datetime | None:
    """Parse TARGET_DATETIME from .env (e.g. '20.2.2026 12:00') as Israel time, or None if missing/invalid."""
    if not TARGET_DATETIME_STR:
        return None
    try:
        date_part, time_part = TARGET_DATETIME_STR.split(None, 1)
        d, m, y = map(int, date_part.split("."))
        h, mi = map(int, time_part.split(":")[:2])
        naive = datetime(y, m, d, h, mi)
        return naive.replace(tzinfo=ISRAEL_TZ)
    except Exception:
        return None


def _current_interval() -> int:
    """Use 1 second when within 15 minutes before target time; else INTERVAL."""
    target = _target_datetime()
    if target is None:
        return INTERVAL
    now = datetime.now(ISRAEL_TZ)
    window_start = target - timedelta(minutes=15)
    if window_start <= now <= target:
        return 1
    return INTERVAL


def _event_url() -> str:
    year = datetime.now(ISRAEL_TZ).year
    return f"https://www.feliz.co.il/event/congress-{year}/"


def send_telegram(message: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("[Telegram] Skipped (no token/chat_id)")
        return

    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    response = requests.post(
        endpoint,
        json={"chat_id": chat_id, "text": message},
        timeout=10,
    )
    print(f"[Telegram] {response.status_code} {response.reason}")
    print(f"[Telegram] response: {response.text}")
    response.raise_for_status()


def fetch_html(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; FelizWatcher/1.0)"}
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    return response.text


def main():
    was_blocked = None  # None = first run (send current status); then True/False

    while True:
        try:
            url = _event_url()
            html = fetch_html(url)
            still_blocked = BLOCK_TEXT in html  # True = registration closed (password page)
            now_dt = datetime.now(ISRAEL_TZ)
            now_str = _israel_time_str(now_dt)

            if still_blocked:
                print(f"[OK] Registration closed (password-protected) | {now_str}")
                if was_blocked is None or was_blocked is False:
                    message = (
                        f"🔒 ההרשמה לקונגרס נסגרה\n{url}\n"
                        f"📅 {_israel_date_str(now_dt)}\n🕐 {_israel_time_only_str(now_dt)}"
                    )
                    send_telegram(message)
            else:
                print(f"[OK] Registration open | {now_str}")
                if was_blocked is None or was_blocked:
                    message = (
                        f"🔓 ההרשמה לקונגרס נפתחה\n{url}\n"
                        f"📅 {_israel_date_str(now_dt)}\n🕐 {_israel_time_only_str(now_dt)}"
                    )
                    send_telegram(message)

            was_blocked = still_blocked

        except Exception as e:
            now_str = _israel_time_str(datetime.now(ISRAEL_TZ))
            print(f"[ERROR] {now_str} - {e}")

        interval = _current_interval()
        time.sleep(interval)


if __name__ == "__main__":
    main()
