import os
import re
import time
import hashlib
import requests
import difflib
from datetime import datetime
from dotenv import load_dotenv

# Telegram message length limit
MAX_MESSAGE_LENGTH = 4096

# Load .env from the project root (same dir as this script) so it works when run from WebStorm/IDE
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
URL = os.environ.get("URL", "https://www.latino.co.il/r/")
# When this text is on the page, registration is closed. When it disappears, registration opened.
KEYWORD = os.environ.get("KEYWORD", "Registration will open on")
INTERVAL = int(os.environ.get("INTERVAL_SECONDS", "60"))

STATE_FILE = "/data/state.json"

def send_telegram(text: str) -> None:
    r = requests.post(
        f"https://api.telegram.org/bot{TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "text": text},
        timeout=20,
    )
    r.raise_for_status()

def fetch(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": "g-studio-monitor/1.0"})
    r.raise_for_status()
    return r.text

def _normalize(s: str) -> str:
    """Lowercase and collapse whitespace so we can match regardless of case/formatting."""
    return " ".join(s.lower().split())

def _text_only(html: str) -> str:
    """Strip HTML tags and normalize whitespace for a readable diff."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _describe_changes(old_html: str, new_html: str) -> str:
    """Produce a short readable summary of what changed (text content only). Fits Telegram limit."""
    old_text = _text_only(old_html)
    new_text = _text_only(new_html)
    if old_text == new_text:
        return "(no visible text change; possibly markup/whitespace)"

    parts = []
    matcher = difflib.SequenceMatcher(None, old_text, new_text)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "replace":
            old_snippet = old_text[i1:i2].strip()
            new_snippet = new_text[j1:j2].strip()
            if old_snippet or new_snippet:
                # Truncate long snippets for readability
                o = (old_snippet[:150] + "…") if len(old_snippet) > 150 else old_snippet
                n = (new_snippet[:150] + "…") if len(new_snippet) > 150 else new_snippet
                parts.append(f"− {o}\n+ {n}")
        elif tag == "delete" and old_text[i1:i2].strip():
            parts.append(f"− {old_text[i1:i2].strip()[:150]}")
        elif tag == "insert" and new_text[j1:j2].strip():
            parts.append(f"+ {new_text[j1:j2].strip()[:150]}")

    result = "\n\n".join(parts) if parts else "(no visible text change)"
    if len(result) > MAX_MESSAGE_LENGTH - 400:
        result = result[: MAX_MESSAGE_LENGTH - 400] + "\n... (truncated)"
    return result

def main():
    last_page_hash = None
    last_page_html = None

    while True:
        last_check = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            html = fetch(URL)
            page_hash = hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest()

            if last_page_hash is not None and page_hash != last_page_hash:
                change_desc = _describe_changes(last_page_html, html)
                msg = f"📄 Something changed in the page:\n{URL}\n\nChanges (text):\n{change_desc}"
                if len(msg) > MAX_MESSAGE_LENGTH:
                    msg = msg[: MAX_MESSAGE_LENGTH - 20] + "\n... (truncated)"
                send_telegram(msg)
            last_page_hash = page_hash
            last_page_html = html

            # Alert when KEYWORD is missing (registration opened). Case-insensitive, ignores extra whitespace.
            html_normalized = _normalize(html)
            keyword_normalized = _normalize(KEYWORD)
            opened = (KEYWORD != "" and keyword_normalized not in html_normalized)

            if opened:
                send_telegram(f"🚨 Registration to Israel Congress looks OPEN: {URL}")

            status = "open" if opened else "closed"
            print(f"Registration is {status}, last check was: {last_check}")

        except Exception as e:
            # Don’t spam on errors; just wait and retry
            print(f"The website is unreachable, last check was: {last_check}")
            print(f"[warn] {e}")
            send_telegram(f"⚠️ Website unreachable: {URL}\nError: {e}")

        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
