#!/usr/bin/env python3
import os, json, time, datetime as dt
import requests

STATUS_PATH = "status.json"

def load_status():
    if os.path.exists(STATUS_PATH):
        with open(STATUS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_status(j):
    with open(STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(j, f, indent=2, ensure_ascii=False)

def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_ids = [c.strip() for c in (os.getenv("TELEGRAM_CHAT_IDS", "") or "").split(",") if c.strip()]
    force_alert = str(os.getenv("FORCE_ALERT", "false")).lower() in ("1","true","y","yes","on")

    if not token or not chat_ids:
        print("send_telegram: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_IDS; skipping")
        return

    status = load_status()
    alert = bool(status.get("alert_triggered"))
    message = status.get("message") or "AVR alert"

    if not alert and not force_alert:
        print("send_telegram: alert_triggered is False and FORCE_ALERT is not set; nothing to send.")
        return

    api = f"https://api.telegram.org/bot{token}/sendMessage"
    results = []
    for cid in chat_ids:
        try:
            r = requests.post(api, data={"chat_id": cid, "text": message}, timeout=20)
            ok = r.ok
            resp = r.text
            print(f"send_telegram: chat {cid} -> {'ok' if ok else 'fail'}")
            results.append({"chat_id": cid, "ok": ok, "response": resp})
        except Exception as e:
            print(f"send_telegram: chat {cid} -> exception: {e}")
            results.append({"chat_id": cid, "ok": False, "response": f"exception: {e}"})

    status.setdefault("notification", {})
    status["notification"].setdefault("telegram", {})
    status["notification"]["telegram"] = {
        "results": results,
        "time_utc": dt.datetime.utcnow().isoformat() + "Z",
        "recipients": chat_ids,
    }

    save_status(status)

if __name__ == "__main__":
    main()
