#!/usr/bin/env python3
import os
import json
import time
from datetime import datetime
import requests

STATUS_PATH = "status.json"

def log(msg):
    print(f"[INFO] {msg}", flush=True)

def getenv_list(name: str) -> list[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    out = []
    for part in [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]:
        try:
            # int() removes any leading zeros safely from strings; supports negatives (e.g., -100...)
            out.append(int(part))
        except Exception:
            log(f"Skipping invalid chat id token: {part!r}")
    return out

def main():
    tz = os.getenv("TIMEZONE", "America/New_York")
    log(f"Timezone: {tz}, now={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_ids = getenv_list("TELEGRAM_CHAT_IDS")

    if not token or not chat_ids:
        log("No TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_IDS; skipping Telegram send.")
        return

    if not os.path.exists(STATUS_PATH):
        log("status.json not found; skipping Telegram send.")
        return

    with open(STATUS_PATH, "r", encoding="utf-8") as f:
        status = json.load(f)

    counts = status.get("counts", {"60":0,"90":0,"120":0})
    total = status.get("total_ctmr_60_90_120", 0)
    allowed = status.get("allowed_window", False)
    alert_triggered = bool(status.get("alert_triggered", False))
    force_alert = str(os.getenv("FORCE_ALERT","")).strip().lower() in ("1","true","yes")

    if not (alert_triggered or force_alert):
        log("Alert not triggered and FORCE_ALERT not set; nothing to send.")
        # still record that no send happened
        status.setdefault("notification", {}).setdefault("telegram", {}).update({
            "recipients": chat_ids,
            "results": [{"chat_id": cid, "sent": False, "reason": "not_triggered"} for cid in chat_ids],
            "time_utc": datetime.utcnow().isoformat() + "Z",
        })
        with open(STATUS_PATH, "w", encoding="utf-8") as f:
            json.dump(status, f, indent=2, ensure_ascii=False)
        return

    text = (
        "AVR CT/MR backlog\n"
        f"60m={counts.get('60',0)}, 90m={counts.get('90',0)}, 120m={counts.get('120',0)} "
        f"(total={total})\n"
        f"Window={'allowed' if allowed else 'not allowed'} • "
        f"{'FORCED' if force_alert and not alert_triggered else 'AUTO'}"
    )

    api = f"https://api.telegram.org/bot{token}/sendMessage"
    results = []
    for cid in chat_ids:
        try:
            r = requests.post(api, json={"chat_id": cid, "text": text, "disable_notification": False}, timeout=20)
            ok = r.ok and r.json().get("ok", False)
            results.append({"chat_id": cid, "sent": ok, "status": r.status_code, "body": r.text[:300]})
            log(f"Sent to {cid}: ok={ok} status={r.status_code}")
            time.sleep(0.3)
        except Exception as e:
            results.append({"chat_id": cid, "sent": False, "error": str(e)})
            log(f"Send error for {cid}: {e}")

    status.setdefault("notification", {}).setdefault("telegram", {}).update({
        "recipients": chat_ids,
        "results": results,
        "time_utc": datetime.utcnow().isoformat() + "Z",
    })
    with open(STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2, ensure_ascii=False)
    log("Telegram step complete and status.json updated.")

if __name__ == "__main__":
    main()
