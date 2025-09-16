#!/usr/bin/env python3
"""
AVR Radiologist Alerts - monitor.py (Telegram-only, robust login + CT/MR parser)

- Logs into AVR (handles Index.aspx -> Login.aspx, meta/JS redirects, ASP.NET inputs)
- Fetches the Worklist page (excludes Completed Studies)
- Counts only CT/MR aged 60–89, 90–119, 120+ minutes (each CT/MR in a cell counted)
- Writes status.json used by Telegram sender
- Saves debug snapshots: docs/last_page.html and docs/last_counts.csv

Environment:
  AVR_USERNAME, AVR_PASSWORD, TIMEZONE, FORCE_ALERT
"""

import os
import re
import csv
import json
import pytz
import yaml
import datetime as dt
from typing import Tuple, Dict, Any, List, Optional

import requests
from bs4 import BeautifulSoup

# ---------------- Config ----------------

BASE_URLS = [
    "https://www.avrteleris.com/AVR",
    "https://avrteleris.com/AVR",
]

INDEX_PATH = "Index.aspx"
WORKLIST_PATHS = [
    "Forms/Worklist/worklist.aspx",
    "Forms/Worklist/Worklist.aspx",  # case variation
]

LOGIN_CANDIDATES = [
    "Login.aspx",
    "login.aspx",
    "Forms/Login.aspx",
    "Account/Login.aspx",
    "Default.aspx",
]

DEFAULT_TZ = "America/New_York"
DEFAULT_THRESHOLD = 20

UA = "Mozilla/5.0 (X11; Linux x86_64) AVR Monitor"

# ---------------- Helpers ----------------

def env_truthy(name: str, default: str = "false") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def now_in_tz(tzname: str) -> dt.datetime:
    tz = pytz.timezone(tzname)
    return dt.datetime.now(tz)

def allowed_window(now_local: dt.datetime) -> bool:
    # Mon–Fri 6:00 pm–11:59 pm; Sat 4:00 am–11:59 pm; Sun 12:00 am–9:00 pm
    wd = now_local.weekday()  # Mon=0 .. Sun=6
    t = now_local.time()

    def between(a: dt.time, b: dt.time) -> bool:
        return a <= t <= b

    if 0 <= wd <= 4:
        return between(dt.time(18, 0), dt.time(23, 59, 59))
    if wd == 5:
        return between(dt.time(4, 0), dt.time(23, 59, 59))
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

# ---------------- Login helpers ----------------

def _abs_url(base: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if path.startswith("/"):
        from urllib.parse import urlsplit, urlunsplit
        sp = urlsplit(base)
        return urlunsplit((sp.scheme, sp.netloc, path, "", ""))
    return f"{base.rstrip('/')}/{path.lstrip('/')}"

def _extract_meta_js_redirect(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for m in soup.find_all("meta"):
        if m.get("http-equiv", "").lower() == "refresh":
            content = m.get("content", "")
            m2 = re.search(r"url=(.+)", content, flags=re.I)
            if m2:
                return m2.group(1).strip().strip("'\"")
    m = re.search(r"location\.(?:href|replace)\(['\"]([^'\"]+)['\"]\)", html, flags=re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r"window\.location\s*=\s*['\"]([^'\"]+)['\"]", html, flags=re.I)
    if m:
        return m.group(1).strip()
    return None

def _find_login_form(soup: BeautifulSoup):
    for form in soup.find_all("form"):
        if form.find("input", {"type": "password"}):
            return form
    return None

def _build_form_payload(form, username: str, password: str) -> Dict[str, str]:
    payload: Dict[str, str] = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        payload[name] = inp.get("value", "")

    def set_best(names: List[str], value: str) -> bool:
        ok = False
        for cand in names:
            for k in list(payload.keys()):
                if k.lower() == cand.lower():
                    payload[k] = value
                    ok = True
        return ok

    user_set = set_best(["username", "user", "userid", "login", "txtusername", "ctl00$maincontent$txtusername"], username)
    pass_set = set_best(["password", "pwd", "pass", "txtpassword", "ctl00$maincontent$txtpassword"], password)

    if not user_set:
        for inp in form.find_all("input"):
            t = (inp.get("type") or "").lower()
            if t in ("text", "email"):
                n = inp.get("name")
                if n:
                    payload[n] = username
                    user_set = True
                    break
    if not pass_set:
        for inp in form.find_all("input"):
            t = (inp.get("type") or "").lower()
            if t == "password":
                n = inp.get("name")
                if n:
                    payload[n] = password
                    pass_set = True
                    break

    for inp in form.find_all("input"):
        if (inp.get("type") or "").lower() in ("submit", "button"):
            n = inp.get("name")
            if n and n not in payload:
                payload[n] = inp.get("value", "") or "Login"
    return payload

def _get_worklist(session: requests.Session, base: str, headers: Dict[str, str]) -> str:
    last_exc = None
    for p in WORKLIST_PATHS:
        try:
            r = session.get(_abs_url(base, p), headers=headers, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_exc = e
    if last_exc:
        raise last_exc
    raise RuntimeError("Unable to fetch Worklist")

def try_login_and_fetch_worklist(session: requests.Session, username: str, password: str) -> Tuple[str, str]:
    headers = {"User-Agent": UA}
    last_error = ""

    for base in BASE_URLS:
        try:
            idx_url = f"{base}/{INDEX_PATH}"
            r = session.get(idx_url, headers=headers, timeout=30, allow_redirects=True)
            r.raise_for_status()
            html = r.text

            redir = _extract_meta_js_redirect(html)
            if redir:
                r = session.get(_abs_url(base, redir), headers=headers, timeout=30, allow_redirects=True)
                r.raise_for_status()
                html = r.text

            if ("Logout" in html and "Worklist" in html) or "Logged In:" in html:
                return base, _get_worklist(session, base, headers)

            soup = BeautifulSoup(html, "html.parser")
            form = _find_login_form(soup)

            if not form:
                for cand in LOGIN_CANDIDATES:
                    url = _abs_url(base, cand)
                    r2 = session.get(url, headers=headers, timeout=30, allow_redirects=True)
                    r2.raise_for_status()
                    html2 = r2.text
                    red = _extract_meta_js_redirect(html2)
                    if red:
                        r2 = session.get(_abs_url(base, red), headers=headers, timeout=30, allow_redirects=True)
                        r2.raise_for_status()
                        html2 = r2.text
                    soup2 = BeautifulSoup(html2, "html.parser")
                    form = _find_login_form(soup2)
                    if form:
                        html = html2
                        break

            if not form:
                last_error = "No login form found"
                continue

            payload = _build_form_payload(form, username, password)
            action = form.get("action") or ""
            post_url = _abs_url(base, action or "Login.aspx")
            headers_post = dict(headers)
            headers_post["Referer"] = r.url

            r3 = session.post(post_url, data=payload, headers=headers_post, timeout=30, allow_redirects=True)
            r3.raise_for_status()
            html3 = r3.text

            red3 = _extract_meta_js_redirect(html3)
            if red3:
                r3 = session.get(_abs_url(base, red3), headers=headers, timeout=30, allow_redirects=True)
                r3.raise_for_status()

            return base, _get_worklist(session, base, headers)

        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"

    raise RuntimeError(f"Failed to login/fetch Worklist. Last error: {last_error}")

# ---------------- Parser ----------------

def parse_worklist_counts(html: str, now_local: dt.datetime, tz) -> Tuple[Dict[str, int], Dict[str, Any]]:
    """
    Parse only the Worklist table (header includes 'Study Requested' and not 'Report Out Time').
    Count only CT/MR; multiple CT/MR in a single cell counted individually.
    Buckets: 60=[60..89], 90=[90..119], 120=120+; ignore <60.
    """
    soup = BeautifulSoup(html, "html.parser")

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

        parts = [p.strip() for p in re.split(r"[;,/]|\\band\\b", study_upper) if p.strip()]
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

        inc = len(parts)
        counts[bucket] += inc
        considered += inc
        included_rows.append({
            "bucket": bucket,
            "age_min": minutes,
            "patient": patient,
            "study_cell": study_cell_raw,
        })

    debug = {
        "rows_seen": rows_seen,
        "ctmr_considered": considered,
        "errors": errors,
        "included_rows": included_rows,
    }
    return counts, debug

# ---------------- Main ----------------

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

    cfg = read_yaml("config.yml", {}) or {}
    threshold = int(cfg.get("threshold_total", DEFAULT_THRESHOLD))

    contacts = read_yaml("contacts.yml", []) or []
    contact_names = [c.get("name") for c in contacts if isinstance(c, dict) and c.get("name")]
    if not contact_names:
        contact_names = ["Reed", "Bargo", "Croce"]

    # Fetch Worklist
    errors: List[str] = []
    html = ""
    base_used = ""

    try:
        with requests.Session() as s:
            s.headers.update({"User-Agent": UA})
            base_used, html = try_login_and_fetch_worklist(s, username, password)
    except Exception as e:
        err = f"fetch: {type(e).__name__}: {e}"
        print(f"[ERROR] {err}")
        errors.append(err)

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

    # Debug snapshots
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

    # Status
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
            # scripts/send_telegram.py will append delivery results under "telegram"
            "recipients_sent": [],
            "recipients_failed": [],
            "errors": [],
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
