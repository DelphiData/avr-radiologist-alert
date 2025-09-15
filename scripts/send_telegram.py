#!/usr/bin/env python3
import os, json, time, datetime, sys
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_IDS_RAW = os.environ.get("TELEGRAM_CHAT_IDS", "").strip()
CHAT_IDS = [c.strip() for c in CHAT_IDS_RAW.split(",") if c.strip()]
STATUS_PATH = "status.json"

def send_message(chat_id: str, text: str):
    base = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
        # plain text is safest across clients; skip parse_mode
    }
    data = urlencode(payload).encode("utf-8")
    req = Request(base, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urlopen(req, timeout=30) as r:
            return True, r.read().decode("utf-8")
    except HTTPError as e:
        return False, f"HTTPError {e.code}: {e.read().decode('utf-8', errors='ignore')}"
    except URLError as e:
        return False, f"URLError: {e.reason}"
    except Exception as e:
        return False, f"Error: {e}"

def load_status():
    if not os.path.exists(STATUS_PATH):
        print("send_telegram: status.json not found; nothing to send.")
        return None
    with open(STATUS_PATH, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception as e:
            print(f"send_telegram: failed to parse status.json: {e}")
            return None

def extract_message(data):
    # Prefer an explicit message from your monitor.py if present
    msg = data.get("message") or data.get("alert", {}).get("message")
    if msg:
        return msg

    # Otherwise synthesize a clear message from the typical fields
    counts = data.get("counts", {})
    c60 = counts.get("60m") or counts.get("ctmr60") or counts.get("ct_60", 0)  # be tolerant
    c90 = counts.get("90m") or counts.get("ctmr90") or counts.get("ct_90", 0)
    c120 = counts.get("120m") or counts.get("ctmr120") or counts.get("ct_120", 0)
    total = sum(int(x) for x in [c60 or 0, c90 or 0, c120 or 0])
    threshold = data.get("threshold", 20)
    return (
        f"AVR Radiologist Alerts: CT/MR backlog 60/90/120 = "
        f"{int(c60 or 0)}/{int(c90 or 0)}/{int(c120 or 0)} (Total {total}, threshold {threshold}). "
        f"Review queue: https://avrteleris.com/AVR/Index.aspx"
    )

def main():
    if not BOT_TOKEN or not CHAT_IDS:
        print("send_telegram: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_IDS missing; skip.")
        return 0

    data = load_status()
    if not data:
        return 0

    # Decide whether to notify
    alert_triggered = (
        data.get("alert_triggered")
        or data.get("alert", {}).get("triggered")
        or data.get("alert", {}).get("trigger")
    )
    window_allowed = data.get("window_allowed", True)
    # If monitor.py already suppressed messages by window/threshold, this script will still run but only send when alert_triggered is true.
    if not alert_triggered:
        print("send_telegram: alert_triggered is False; nothing to send.")
        return 0

    msg = extract_message(data)
    results = []
    for chat_id in CHAT_IDS:
        ok, resp = send_message(chat_id, msg)
        results.append({"chat_id": chat_id, "ok": ok, "response": resp})
        print(f"send_telegram: chat {chat_id} -> {'ok' if ok else 'error'}")

        # mild pacing to avoid flood limits
        time.sleep(0.5)

    # Persist results back into status.json for visibility
    ts = datetime.datetime.utcnow().isoformat() + "Z"
    notif = data.get("notification", {})
    notif["telegram"] = {"results": results, "time_utc": ts, "recipients": CHAT_IDS}
    data["notification"] = notif
    try:
        with open(STATUS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"send_telegram: failed writing status.json: {e}")

    # Exit nonzero if all failed (keeps Actions logs obvious)
    return 0 if any(r["ok"] for r in results) else 1

if __name__ == "__main__":
    sys.exit(main())
