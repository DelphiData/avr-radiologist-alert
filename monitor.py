import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from twilio.rest import Client
import yaml

def load_yaml(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or default
    return default

def allowed_window_now(tz):
    now = datetime.now(tz)
    wd = now.weekday()
    h = now.hour
    if 0 <= wd <= 4:
        return 18 <= h < 23
    if wd == 5:
        return h >= 4
    if wd == 6:
        return h < 21
    return False

def truthy(val: str) -> bool:
    if val is None:
        return False
    v = str(val).strip().lower()
    return v not in ("", "0", "false", "no", "off", "none", "null")

def main():
    tz_name = os.environ.get("TIMEZONE", "America/New_York")
    tz = ZoneInfo(tz_name)
    config = load_yaml("config.yml", {})
    contacts = load_yaml("contacts.yml", {"radiologists": []})

    threshold = int(config.get("threshold_total", 20))
    allowed_desc = config.get("allowed_window_desc", "Mon–Fri 6pm–11pm, Sat 4am–Sun 9pm")
    buckets = config.get("time_bucket_minutes", [60, 90, 120])

    c60 = 8
    c90 = 7
    c120 = 6
    counts = {str(buckets[0]): c60, str(buckets[1]): c90, str(buckets[2]): c120}
    total = c60 + c90 + c120

    allowed = allowed_window_now(tz)
    force_alert = truthy(os.environ.get("FORCE_ALERT", ""))

    twilio_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
    twilio_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
    twilio_from = os.environ.get("TWILIO_FROM_NUMBER", "")

    recipients = [{"name": r.get("name", ""), "phone": str(r.get("phone", ""))} for r in contacts.get("radiologists", []) if r.get("phone")]
    sent = []
    failed = []
    notify_errors = []
    last_msg_sid = ""
    last_status = ""
    last_error_code = None
    last_error_message = ""

    should_alert = ((allowed and total >= threshold) or force_alert)

    msg_prefix = "FORCED " if force_alert else ""
    msg = f"{msg_prefix}CT/MR backlog 60/90/120 total={total} (>= {threshold} threshold). Notifying: " + ", ".join([r["name"] for r in recipients]) if recipients else "No recipients configured"

    print(f"[INFO] Timezone: {tz_name}, now={datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Allowed window now: {allowed} ({allowed_desc})")
    print(f"[INFO] Counts: 60={c60}, 90={c90}, 120={c120}, total={total}, threshold={threshold}")
    print(f"[INFO] Force alert: {force_alert}")
    print(f"[INFO] Recipients configured: {len(recipients)}")

    if should_alert and twilio_sid and twilio_token and twilio_from and recipients:
        try:
            client = Client(twilio_sid, twilio_token)
            for r in recipients:
                try:
                    m = client.messages.create(to=r["phone"], from_=twilio_from, body=msg)
                    sent.append({"name": r["name"], "phone": r["phone"]})
                    last_msg_sid = getattr(m, "sid", "") or ""
                    last_status = getattr(m, "status", "") or ""
                    print(f"[INFO] SMS queued for {r['name']} {r['phone']} sid={last_msg_sid} status={last_status}")
                except Exception as ex:
                    failed.append({"name": r["name"], "phone": r["phone"], "error": str(ex)})
                    print(f"[ERROR] Failed to send to {r['name']} {r['phone']}: {ex}")
        except Exception as ex:
            notify_errors.append(str(ex))
            print(f"[ERROR] Twilio client error: {ex}")
    elif should_alert and not (twilio_sid and twilio_token and twilio_from):
        notify_errors.append("Twilio not configured in secrets")
        print("[WARN] Twilio not configured; set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER")
    elif should_alert and not recipients:
        notify_errors.append("No recipients in contacts.yml")
        print("[WARN] No recipients found in contacts.yml")

    data = {
        "timestamp": datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": tz_name,
        "allowed_window": allowed,
        "counts": counts,
        "total_ctmr_60_90_120": total,
        "alert_triggered": bool(should_alert),
        "message": msg,
        "config": {"threshold_total": threshold, "allowed_window_desc": allowed_desc},
        "contacts_total": len(recipients),
        "notification": {"recipients_sent": sent, "recipients_failed": failed, "errors": notify_errors},
        "twilio": {
            "from_e164": twilio_from or "",
            "message_sid": last_msg_sid,
            "status": last_status,
            "error_code": last_error_code,
            "error_message": last_error_message
        },
        "scrape": {"rows_seen": None, "ctmr_considered": None, "errors": []}
    }

    with open("status.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print("[INFO] Wrote status.json")

if __name__ == "__main__":
    main()
