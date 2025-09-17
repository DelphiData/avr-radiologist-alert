#!/usr/bin/env python3
"""
AVR Radiologist Alerts - monitor.py (www-only, robust login, CT/MR parser, Telegram-ready)

- Logs into AVR (handles Index.aspx, meta/JS redirects, ASP.NET hidden fields)
- Fetches the Worklist page (NOT Completed Studies)
- Counts only CT/MR studies aged 60–89, 90–119, 120+ minutes
  • Each CT or MR/MRI mention in a study cell is counted individually
- Writes status.json used by scripts/send_telegram.py
- Saves debug snapshots: docs/last_page.html, docs/last_counts.csv, plus docs/debug_*.html

Env:
  AVR_USERNAME, AVR_PASSWORD
  TIMEZONE (default America/New_York)
  FORCE_ALERT (true/false)
"""

import os
import re
import csv
import json
import pytz
import yaml
import datetime as dt
from typing import Optional, Tuple, Dict, Any, List

import requests
from bs4 import BeautifulSoup

# ---------------- Config ----------------

# IMPORTANT: Use ONLY the www host (bare domain returns 404 on Login.aspx)
BASE_URL = "https://www.avrteleris.com/AVR"

INDEX_PATH = "Index.aspx"

# IIS can serve either casing; try both
WORKLIST_PATHS = [
    "Forms/Worklist/Worklist.aspx",
    "Forms/Worklist/worklist.aspx",
]

# Common login endpoints if the login form isn’t on Index.aspx
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
    wd = now_local.weekday()
    t = now_local.time()
    def between(a: dt.time, b: dt.time) -> bool:
        return a <= t <= b
    if 0 <= wd <= 4:  # Mon–Fri
        return between(dt.time(18, 0), dt.time(23, 59, 59))
    if wd == 5:      # Sat
        return between(dt.time(4, 0), dt.time(23, 59, 59))
    return between(dt.time(0, 0), dt.time(21, 0))  # Sun

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
            mm = re.search(r"url=(.+)", content, flags=re.I)
            if mm:
                return mm.group(1).strip().strip("'\"")
    # JS redirects
    mm = re.search(r"location\.(?:href|replace)\(['\"]([^'\"]+)['\"]\)", html, flags=re.I)
    if mm:
        return mm.group(1).strip()
    mm = re.search(r"window\.location\s*=\s*['\"]([^'\"]+)['\"]", html, flags=re.I)
    if mm:
        return mm.group(1).strip()
    return None

def _find_login_form(soup: BeautifulSoup):
    for form in soup.find_all("form"):
        if form.find("input", {"type": "password"}):
            return form
    return None

def _build_form_payload(form, username: str, password: str) -> Dict[str, str]:
    payload: Dict[str, str] = {}
    for inp in form.find_all("input"):
        n = inp.get("name")
        if not n:
            continue
        payload[n] = inp.get("value", "")

    def set_best(names: List[str], value: str) -> bool:
        ok = False
        for cand in names:
            for k in list(payload.keys()):
                if k.lower() == cand.lower():
                    payload[k] = value
                    ok = True
        return ok

    user_set = set_best(["username","user","userid","login","txtusername","ctl00$maincontent$txtusername"], username)
    pass_set = set_best(["password","pwd","pass","txtpassword","ctl00$maincontent$txtpassword"], password)

    if not user_set:
        for inp in form.find_all("input"):
            if (inp.get("type") or "").lower() in ("text","email"):
                n = inp.get("name")
                if n:
                    payload[n] = username
                    user_set = True
                    break
    if not pass_set:
        for inp in form.find_all("input"):
            if (inp.get("type") or "").lower() == "password":
                n = inp.get("name")
                if n:
                    payload[n] = password
                    pass_set = True
                    break

    # Ensure a submit value if present
    for inp in form.find_all("input"):
        if (inp.get("type") or "").lower() in ("submit","button"):
            n = inp.get("name")
            if n and n not in payload:
                payload[n] = inp.get("value","") or "Login"
    return payload

# ---------------- Login + fetch ----------------

def try_login_and_fetch_worklist(session: requests.Session, username: str, password: str, dbg: bool = True) -> Tuple[str, str]:
    """
    Returns (base_url, worklist_html) using the www host only.
    Strategy:
      - Try Worklist directly (both casings)
      - GET Index.aspx (follow meta/JS redirects)
      - Find login form or probe known login endpoints
      - POST with ASP.NET fields
      - GET Worklist again
    """
    headers = {"User-Agent": UA}
    base = BASE_URL
    first_dump_done = False

    def dump(name: str, html: str):
        nonlocal first_dump_done
        if not dbg:
            return
        os.makedirs("docs", exist_ok=True)
        safe_write_text(f"docs/{name}", html)
        if not first_dump_done:
            safe_write_text("docs/debug_first_response.html", html)
            first_dump_done = True

    def get_follow(url: str) -> requests.Response:
        r = session.get(url, headers=headers, timeout=30, allow_redirects=True)
        r.raise_for_status()
        html = r.text
        red = _extract_meta_js_redirect(html)
        if red:
            r = session.get(_abs_url(base, red), headers=headers, timeout=30, allow_redirects=True)
            r.raise_for_status()
        return r

    # A) Try Worklist directly (both casings)
    for wp in WORKLIST_PATHS:
        wl_url = _abs_url(base, wp)
        r = get_follow(wl_url)
        html = r.text
        dump("debug_wl_attempt.html", html)
        if ("Logout" in html or "Logged In:" in html) and "Worklist" in html:
            return base, html

    # B) Index.aspx
    idx_url = _abs_url(base, INDEX_PATH)
    r = get_follow(idx_url)
    html = r.text
    dump("debug_index.html", html)

    # Already logged in?
    if ("Logout" in html or "Logged In:" in html) and "Worklist" in html:
        w = get_follow(_abs_url(base, WORKLIST_PATHS[0]))
        return base, w.text

    soup = BeautifulSoup(html, "html.parser")
    form = _find_login_form(soup)

    # C) Probe specific login endpoints if no form yet
    if not form:
        for cand in LOGIN_CANDIDATES:
            r2 = get_follow(_abs_url(base, cand))
            html2 = r2.text
            dump("debug_login_page.html", html2)
            soup2 = BeautifulSoup(html2, "html.parser")
            form = _find_login_form(soup2)
            if form:
                html = html2
                break

    if not form:
        raise RuntimeError("No login form found")

    # D) POST credentials
    payload = _build_form_payload(form, username, password)
    action = form.get("action") or ""
    post_url = _abs_url(base, action or "Login.aspx")
    headers_post = dict(headers)
    headers_post["Referer"] = r.url

    r3 = session.post(post_url, data=payload, headers=headers_post, timeout=30, allow_redirects=True)
    r3.raise_for_status()
    html3 = r3.text
    dump("debug_after_login_post.html", html3)

    red3 = _extract_meta_js_redirect(html3)
    if red3:
        r3 = get_follow(_abs_url(base, red3))
        dump("debug_after_login_redirect.html", r3.text)

    # E) Fetch Worklist finally
    for wp in WORKLIST_PATHS:
        wl_url = _abs_url(base, wp)
        w = get_follow(wl_url)
        html_wl = w.text
        dump("debug_wl_final.html", html_wl)
        if "Worklist" in html_wl:
            return base, html_wl

    raise RuntimeError("Login POST ok but Worklist not reachable")

# ---------------- Parser ----------------

def parse_worklist_counts(html: str, now_local: dt.datetime, tz) -> Tuple[Dict[str, int], Dict[str, Any]]:
    """
    Parse only the Worklist table (header includes 'Study Requested' and NOT 'Report Out Time').
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
        # Expected like: "Sep 16, 2025" and "19:03:00"
        try:
            dt_naive = dt.datetime.strptime(f"{date_str} {time_str}", "%b %d, %Y %H:%M:%S")
            return tz.localize(dt_naive)
        except Exception as e:
            errors.append(f"date-parse: {e} for '{date_str} {time_str}'")
            return None

    for tr in work_table.find_all("tr"):
        if tr.find("th"):  # skip header row
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

        # Split on common separators or the word AND, count CT/MR tokens
        parts = [p.strip() for p in re.split(r"[;,/]|(?:\bAND\b)", study_upper) if p.strip()]
        # Count CT and MR/MRI mentions across parts
        inc = 0
        for p in parts:
            # Strict word boundaries to avoid matching CTA, etc.
            inc += len(re.findall(r"\bCT\b", p))
            inc += len(re.findall(r"\bMRI?\b", p))  # MR or MRI

        if inc == 0:
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

        counts[bucket] += inc
        considered += inc
        included_rows.append({
            "bucket": bucket,
            "age_min": minutes,
            "patient": patient,
            "study_cell": study_cell_raw,
            "ct_mr_count_in_row": inc,
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

    errors: List[str] = []
    html = ""
    base_used = BASE_URL

    try:
        with requests.Session() as s:
            s.headers.update({"User-Agent": UA})
            base_used, html = try_login_and_fetch_worklist(s, username, password, dbg=True)
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
            w.writerow(["bucket", "age_min", "patient", "study_cell", "ct_mr_count_in_row"])
            for r in debug.get("included_rows", []):
                w.writerow([
                    r.get("bucket",""),
                    r.get("age_min",""),
                    r.get("patient",""),
                    r.get("study_cell",""),
                    r.get("ct_mr_count_in_row",""),
                ])
    except Exception as e:
        print(f"[WARN] Failed to save debug snapshots: {e}")

    # Status + alert decision
    alert_ok = (total >= threshold) and allowed
    if force_alert:
        alert_ok = True

    # Include per-bucket counts in message as requested
    message = (
        f"CT/MR backlog: 60m={counts['60']}, 90m={counts['90']}, 120m={counts['120']} "
        f"(total={total} {'>=' if total >= threshold else '<'} {threshold}). "
        f"Notifying: {', '.join(contact_names)}"
    )

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
            "telegram": {
                "recipients": [],   # filled by scripts/send_telegram.py
                "results": [],      # filled by scripts/send_telegram.py
                "time_utc": "",     # filled by scripts/send_telegram.py
            }
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
