import os
import time
import hashlib
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load .env from the project root (same dir as this script) so it works when run from WebStorm/IDE
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
URL = os.environ.get("URL", "https://www.latino.co.il/r/")
KEYWORD = os.environ.get("KEYWORD", "")
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

def main():
    last_page_hash = None

    while True:
        last_check = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            html = fetch(URL)
            page_hash = hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest()

            if last_page_hash is not None and page_hash != last_page_hash:
                send_telegram(f"📄 Something changed in the page: {URL}")
            last_page_hash = page_hash

            # Alert when KEYWORD is missing (registration opened)
            opened = (KEYWORD != "" and KEYWORD not in html)

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
