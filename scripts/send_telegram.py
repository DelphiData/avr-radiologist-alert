#!/usr/bin/env python3
import os, json, time, datetime, sys, numbers
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_IDS_RAW = os.environ.get("TELEGRAM_CHAT_IDS", "").strip()
CHAT_IDS = [c.strip() for c in CHAT_IDS_RAW.split(",") if c.strip()]
STATUS_PATH = "status.json"
FORCE_ALERT = os.environ.get("FORCE_ALERT","").lower() in ("1","true","yes","on")

def send_message(chat_id: str, text: str):
    base = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
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
        return None
    try:
        with open(STATUS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"send_telegram: failed to parse status.json: {e}")
        return None

def _to_int(x, default=0):
    try:
        if isinstance(x, numbers.Number): return int(x)
        return int(str(x).strip())
    except Exception:
        return default

def extract_counts(data):
    # tolerant scan for 60/90/120 keys
    c60=c90=c120=0
    def walk(o):
        nonlocal c60,c90,c120
        if isinstance(o, dict):
            for k,v in o.items():
                lk=str(k).lower()
                if any(b in lk for b in ("60","90","120")) and not isinstance(v,(dict,list)):
                    if "60" in lk: c60 += _to_int(v,0)
                    if "90" in lk: c90 += _to_int(v,0)
                    if "120" in lk: c120 += _to_int(v,0)
                else:
                    walk(v)
        elif isinstance(o, list):
            for it in o: walk(it)
    walk(data or {})
    return c60,c90,c120

def build_message(data):
    if data:
        msg = data.get("message") or data.get("alert", {}).get("message")
        if msg: return msg
        c60,c90,c120 = extract_counts(data)
        total = c60 + c90 + c120
        threshold = _to_int((data.get("threshold") or 20), 20)
        return (f"AVR Radiologist Alerts: CT/MR backlog 60/90/120 = "
                f"{c60}/{c90}/{c120} (Total {total}, threshold {threshold}). "
                f"Review queue: https://avrteleris.com/AVR/Index.aspx")
    # fallback when no status.json
    return ("AVR Radiologist Alerts (test): sending force-alert to verify Telegram. "
            "Review queue: https://avrteleris.com/AVR/Index.aspx")

def main():
    if not BOT_TOKEN or not CHAT_IDS:
        print("send_telegram: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_IDS missing; skip.")
        return 0

    data = load_status()
    alert_triggered = False
    if data:
        alert_triggered = (
            data.get("alert_triggered")
            or data.get("alert", {}).get("triggered")
            or data.get("alert", {}).get("trigger")
            or False
        )

    if not alert_triggered and not FORCE_ALERT:
        print("send_telegram: alert_triggered is False; nothing to send.")
        return 0

    msg = build_message(data)
    results = []
    for chat_id in CHAT_IDS:
        ok, resp = send_message(chat_id, msg)
        results.append({"chat_id": chat_id, "ok": ok, "response": resp})
        print(f"send_telegram: chat {chat_id} -> {'ok' if ok else 'error'}")
        time.sleep(0.5)

    # persist results if we had a status.json
    if data is not None:
        ts = datetime.datetime.utcnow().isoformat() + "Z"
        notif = data.get("notification", {})
        notif["telegram"] = {"results": results, "time_utc": ts, "recipients": CHAT_IDS}
        data["notification"] = notif
        try:
            with open(STATUS_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"send_telegram: failed writing status.json: {e}")

    return 0 if any(r["ok"] for r in results) else 1

if __name__ == "__main__":
    sys.exit(main())
