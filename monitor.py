#!/usr/bin/env python3
"""
AVR Radiologist Alerts - monitor.py

What this script does:
- Logs into AVR
- Fetches the Worklist page (NOT Completed Studies)
- Counts only CT/MR studies that are aged 60–89, 90–119, 120+ minutes
- Writes status.json used by the Telegram sender workflow step
- Saves debug snapshots (docs/last_page.html, docs/last_counts.csv) so we can validate parsing
"""

import os
import re
import csv
import json
import time
import pytz
import yaml
import datetime as dt
from typing import Tuple, Dict, Any, List

import requests
from bs4 import BeautifulSoup


BASE_URLS = [
    "https://www.avrteleris.com/AVR",
    "https://avrteleris.com/AVR",
]
INDEX_PATH = "Index.aspx"
WORKLIST_PATH = "Forms/Worklist/worklist.aspx"

DEFAULT_TZ = "America/New_York"
DEFAULT_THRESHOLD = 20

# ---------- Helpers ----------

def env_truthy(name: str, default: str = "false") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def now_in_tz(tzname: str) -> dt.datetime:
    tz = pytz.timezone(tzname)
    return dt.datetime.now(tz)

def allowed_window(now_local: dt.datetime) -> bool:
    # Windows:
    # - Mon–Fri: 6:00 pm–11:59 pm
    # - Sat: 4:00 am–11:59 pm
    # - Sun: 12:00 am–9:00 pm
    wd = now_local.weekday()  # Mon=0 ... Sun=6
    t = now_local.time()

    def between(t0: dt.time, t1: dt.time) -> bool:
        return (t >= t0) and (t <= t1)

    if 0 <= wd <= 4:  # Mon–Fri
        return between(dt.time(18, 0), dt.time(23, 59, 59))
    if wd == 5:  # Sat
        return between(dt.time(4, 0), dt.time(23, 59, 59))
    # Sun
    return between(dt.time(0, 0), dt.time(21, 0))

def read_yaml(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or default
    except Exception:
        return default

def safe_write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def safe_write_status(status: Dict[str, Any]):
    with open("status.json", "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2, ensure_ascii=False)


# ---------- Login & fetch ----------

def try_login_and_fetch_worklist(session: requests.Session, username: str, password: str) -> Tuple[str, str]:
    """
    Returns (final_base_url, worklist_html).
    Tries both www and bare domains. Handles generic ASP.NET forms by copying inputs.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AVR Monitor",
    }

    last_error = ""
    for base in BASE_URLS:
        try:
            # Step 1: Hit Index.aspx
            idx_url = f"{base}/{INDEX_PATH}"
            r = session.get(idx_url, headers=headers, timeout=30)
            r.raise_for_status()
            html = r.text

            # If already logged in, go to Worklist
            if "Logout" in html or "Worklist" in html:
                wl_url = f"{base}/{WORKLIST_PATH}"
                w = session.get(wl_url, headers=headers, timeout=30)
                w.raise_for_status()
                return base, w.text

            # Otherwise, attempt to find a login form with a password field
            soup = BeautifulSoup(html, "html.parser")
            login_form = None
            for form in soup.find_all("form"):
                if form.find("input", {"type": "password"}):
                    login_form = form
                    break

            if not login_form:
                # Try a direct Worklist GET; some sites redirect to login automatically
                wl_url = f"{base}/{WORKLIST_PATH}"
                w = session.get(wl_url, headers=headers, timeout=30)
                w.raise_for_status()
                if "Logout" in w.text or "Worklist" in w.text:
                    return base, w.text
                # Fall through to parse for login
                soup = BeautifulSoup(w.text, "html.parser")
                login_form = None
                for form in soup.find_all("form"):
                    if form.find("input", {"type": "password"}):
                        login_form = form
                        break

            if not login_form:
                last_error = "No login form found"
                continue

            # Build payload: include all inputs + username/password overrides
            payload = {}
            for inp in login_form.find_all("input"):
                name = inp.get("name")
                if not name:
                    continue
                val = inp.get("value", "")
                payload[name] = val

            # Best-guess username/password field names
            # Try explicit name matches first
            def set_best(name_candidates: List[str], value: str):
                set_ok = False
                for cand in name_candidates:
                    for key in list(payload.keys()):
                        if key.lower() == cand.lower():
                            payload[key] = value
                            set_ok = True
                return set_ok

            user_set = set_best(["username", "user", "userid", "login", "txtUserName", "ctl00$MainContent$txtUserName"], username)
            pass_set = set_best(["password", "pwd", "pass", "txtPassword", "ctl00$MainContent$txtPassword"], password)

            # If not set, heuristically assign to first text/password fields
            if not user_set:
                for inp in login_form.find_all("input"):
                    if inp.get("type", "").lower() in ("text", "email"):
                        n = inp.get("name")
                        if n and ("user" in n.lower() or "login" in n.lower()):
                            payload[n] = username
                            user_set = True
                            break
            if not pass_set:
                for inp in login_form.find_all("input"):
                    if inp.get("type", "").lower() == "password":
                        n = inp.get("name")
                        if n:
                            payload[n] = password
                            pass_set = True
                            break

            action = login_form.get("action") or ""
            if not action.startswith("http"):
                # Resolve relative action
                if action.startswith("/"):
                    post_url = f"{base}{action}"
                else:
                    post_url = f"{base}/{action or INDEX_PATH}"
            else:
                post_url = action

            r2 = session.post(post_url, data=payload, headers=headers, timeout=30)
            r2.raise_for_status()

            # After login, go to Worklist
            wl_url = f"{base}/{WORKLIST_PATH}"
            w = session.get(wl_url, headers=headers, timeout=30)
            w.raise_for_status()
            return base, w.text

        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"

    raise RuntimeError(f"Failed to login/fetch Worklist. Last error: {last_error}")


# ---------- Parser ----------

def parse_worklist_counts(html: str, now_local: dt.datetime, tz) -> Tuple[Dict[str, int], Dict[str, Any]]:
    """
    Only parse the Worklist table; ignore 'Completed Studies'.
    Count only CT/MR; multiple CT/MR in one cell counted individually.
    Buckets: 60=[60..89], 90=[90..119], 120=120+ minutes; ignore < 60.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the Worklist table: must contain "Study Requested" but NOT "Report Out Time"
    work_table = None
    for t in soup.find_all("table"):
        headers = " ".join(th.get_text(" ", strip=True) for th in t.find_all("th"))
        if headers and ("Study Requested" in headers) and ("Report Out Time" not in headers):
            work_table = t
            break

    counts = {"60": 0, "90": 0, "120": 0}
    rows_seen = 0
    considered = 0
    errors: List[str] = []
    included_rows: List[Dict[str, Any]] = []

    if work_table is None:
        errors.append("worklist table not found")
        return counts, {"rows_seen": 0, "ctmr_considered": 0, "errors": errors, "included_rows": []}

    # date format example: "Sep 16, 2025", time example: "19:02:58"
    def parse_req_dt(date_str: str, time_str: str):
        try:
            dt_naive = dt.datetime.strptime(f"{date_str} {time_str}", "%b %d, %Y %H:%M:%S")
            return tz.localize(dt_naive)
        except Exception as e:
            errors.append(f"date-parse: {e} for '{date_str} {time_str}'")
            return None

    for tr in work_table.find_all("tr"):
        if tr.find("th"):
            continue
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        rows_seen += 1
        patient = tds[2].get_text(strip=True)
        date_str = tds[4].get_text(strip=True)
        time_str = tds[5].get_text(strip=True)
        study_cell_raw = tds[6].get_text(" ", strip=True)
        study_upper = study_cell_raw.upper()

        # Split into potential studies; keep only CT/MR
        parts = [p.strip() for p in re.split(r"[;,/]|\\band\\b", study_upper) if p.strip()]
        # Keep CT or MR(I)
        parts = [p for p in parts if re.search(r"\\bCT\\b", p) or re.search(r"\\bMR(I)?\\b", p)]
        if not parts:
            continue

        req_dt = parse_req_dt(date_str, time_str)
        if not req_dt:
            continue

        minutes = int((now_local - req_dt).total_seconds() // 60)
        if minutes < 60:
            continue

        if minutes >= 120:
            bucket = "120"
        elif minutes >= 90:
            bucket = "90"
        else:
            bucket = "60"

        inc = len(parts)  # count each CT/MR listed in the cell
        counts[bucket] += inc
        considered += inc
        included_rows.append({
            "bucket": bucket,
            "age_min": minutes,
            "patient": patient,
            "study_cell": study_cell_raw
        })

    debug = {
        "rows_seen": rows_seen,
        "ctmr_considered": considered,
        "errors": errors,
        "included_rows": included_rows
    }
    return counts, debug


# ---------- Main ----------

def main():
    username = os.getenv("AVR_USERNAME", "").strip()
    password = os.getenv("AVR_PASSWORD", "").strip()
    tzname = os.getenv("TIMEZONE", DEFAULT_TZ)
    force_alert = env_truthy("FORCE_ALERT", "false")

    tz = pytz.timezone(tzname)
    now_local = now_in_tz(tzname)

    print(f"[INFO] Timezone: {tzname}, now={now_local.strftime('%Y-%m-%d %H:%M:%S')}")
    allowed = allowed_window(now_local)
    print(f"[INFO] Allowed window now: {allowed} (Mon–Fri 6pm–11pm, Sat 4am–Sun 9pm)")

    # Load config/contacts (optional)
    cfg = read_yaml("config.yml", {}) or {}
    threshold = int(cfg.get("threshold_total", DEFAULT_THRESHOLD))
    contacts = read_yaml("contacts.yml", []) or []
    contact_names = [c.get("name") for c in contacts if isinstance(c, dict) and c.get("name")]
    if not contact_names:
        # Fallback to known names used in your messages
        contact_names = ["Reed", "Bargo", "Croce"]

    # Twilio presence (we do NOT send SMS here, just record presence)
    tw_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    tw_auth = os.getenv("TWILIO_AUTH_TOKEN", "")
    tw_from = os.getenv("TWILIO_FROM_NUMBER", "")
    twilio_configured = bool(tw_sid and tw_auth and tw_from)
    if not twilio_configured:
        print("[WARN] Twilio not configured; set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER")

    # Fetch Worklist page
    errors: List[str] = []
    html = ""
    base_used = ""

    try:
        with requests.Session() as s:
            base_used, html = try_login_and_fetch_worklist(s, username, password)
    except Exception as e:
        err = f"fetch: {type(e).__name__}: {e}"
        print(f"[ERROR] {err}")
        errors.append(err)

    # Parse counts
    counts = {"60": 0, "90": 0, "120": 0}
    debug = {"rows_seen": 0, "ctmr_considered": 0, "errors": [], "included_rows": []}
    if html:
        counts, debug = parse_worklist_counts(html, now_local, tz)
    else:
        debug["errors"].append("no HTML fetched")

    total = counts["60"] + counts["90"] + counts["120"]
    print(f"[INFO] Counts: 60={counts['60']}, 90={counts['90']}, 120={counts['120']}, total={total}, threshold={threshold}")
    print(f"[INFO] Force alert: {force_alert}")
    print(f"[INFO] Recipients configured: {len(contact_names)}")

    # Save debug snapshots
    try:
        os.makedirs("docs", exist_ok=True)
        if html:
            safe_write_text("docs/last_page.html", html)
        with open("docs/last_counts.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["bucket", "age_min", "patient", "study_cell"])
            for r in debug.get("included_rows", []):
                w.writerow([r.get("bucket",""), r.get("age_min",""), r.get("patient",""), r.get("study_cell","")])
    except Exception as e:
        print(f"[WARN] Failed to save debug snapshots: {e}")

    # Compose status
    alert_ok = (total >= threshold) and allowed
    if force_alert:
        alert_ok = True

    message = f"CT/MR backlog 60/90/120 total={total} (>= {threshold} threshold). Notifying: {', '.join(contact_names)}"

    status: Dict[str, Any] = {
        "timestamp": now_local.strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": tzname,
        "allowed_window": allowed,
        "counts": counts,
        "total_ctmr_60_90_120": total,
        "alert_triggered": alert_ok,
        "message": message,
        "config": {
            "threshold_total": threshold,
            "allowed_window_desc": "Mon–Fri 6pm–11pm, Sat 4am–Sun 9pm",
        },
        "contacts_total": len(contact_names),
        "notification": {
            "recipients_sent": [],
            "recipients_failed": [],
            "errors": ([] if twilio_configured else ["Twilio not configured in secrets"]),
            # 'telegram' results will be appended by scripts/send_telegram.py
        },
        "twilio": {
            "from_e164": tw_from or "",
            "message_sid": "",
            "status": "",
            "error_code": None,
            "error_message": "",
        },
        "scrape": {
            "rows_seen": debug.get("rows_seen"),
            "ctmr_considered": debug.get("ctmr_considered"),
            "errors": (errors + debug.get("errors", [])),
            "base_url": base_used,
        },
    }

    safe_write_status(status)
    print("[INFO] Wrote status.json")


if __name__ == "__main__":
    main()
