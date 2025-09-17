#!/usr/bin/env python3
"""
AVR Radiologist Alerts - monitor.py (complete)

What it does:
- Logs into AVR (www host only), handling Index.aspx, query redirects, meta/JS redirects, ASP.NET hidden fields.
- Tries index.aspx?reporttype=1 first (required by some deployments).
- Fetches the Worklist page (NOT Completed Studies).
- Parses only CT/MR studies and buckets by age: 60–89, 90–119, 120+ minutes. Multiple CT/MR mentions in one row are counted individually.
- Writes status.json for scripts/send_telegram.py.
- ALWAYS writes debug artifacts (even on failures): docs/debug_*.html, docs/last_page.html, docs/last_counts.csv.

Env vars:
  AVR_USERNAME, AVR_PASSWORD
  TIMEZONE (defaults to America/New_York)
  FORCE_ALERT (true/false)

Optional files (if present):
  config.yml     -> { threshold_total: int }
  contacts.yml   -> [ { name: "..." }, ... ]
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

# Index/login candidates (some deployments require reporttype=1)
INDEX_CANDIDATES = [
    "Index.aspx?reporttype=1",
    "index.aspx?reporttype=1",
    "Index.aspx",
    "index.aspx",
]

# Worklist paths (IIS can serve either casing)
WORKLIST_PATHS = [
    "Forms/Worklist/Worklist.aspx",
    "Forms/Worklist/worklist.aspx",
]

# Additional login endpoints to probe if no form is found on Index.aspx
LOGIN_CANDIDATES = [
    "Index.aspx?reporttype=1",
    "index.aspx?reporttype=1",
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
    # Mon–Fri 6:00 pm–11:59:59 pm; Sat 4:00 am–11:59:59 pm; Sun 12:00 am–9:00 pm
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
        f.write(text if text is not None else "")

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
    if not html:
        return None
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

    # Ensure a submit value if present (ASP.NET buttons often need their name present)
    for inp in form.find_all("input"):
        if (inp.get("type") or "").lower() in ("submit","button"):
            n = inp.get("name")
            if n and n not in payload:
                payload[n] = inp.get("value","") or "Login"
    return payload

# ---------------- Login + fetch (always dumps artifacts) ----------------

def try_login_and_fetch_worklist(session: requests.Session, username: str, password: str, dbg: bool = True) -> Tuple[str, str]:
    """
    Returns (base_url, worklist_html) using the www host only.
    ALWAYS dumps HTML snapshots before any raise so artifacts exist even on errors.
    """
    headers = {"User-Agent": UA}
    base = BASE_URL

    def dump(name: str, html: str, url: str = "", status: Optional[int] = None):
        if not dbg:
            return
        os.makedirs("docs", exist_ok=True)
        prefix = ""
        if url or status is not None:
            prefix = f"<!-- url={url} status={status} -->\n"
        safe_write_text(f"docs/{name}", prefix + (html or ""))

    def safe_get(url: str, label: str) -> requests.Response:
        try:
            r = session.get(url, headers=headers, timeout=30, allow_redirects=True)
            dump(f"{label}.html", getattr(r, "text", ""), r.url, r.status_code)
            # Follow meta/JS redirect if present
            html = getattr(r, "text", "")
            red = _extract_meta_js_redirect(html or "")
            if red:
                r2 = session.get(_abs_url(base, red), headers=headers, timeout=30, allow_redirects=True)
                dump(f"{label}_redirect.html", getattr(r2, "text", ""), r2.url, r2.status_code)
                return r2
            return r
        except Exception as e:
            dump(f"{label}_exception.html", f"Exception during GET {url}\n{e}", url, None)
            raise

    def safe_post(url: str, data: Dict[str, str], ref: str, label: str) -> requests.Response:
        try:
            headers_post = dict(headers)
            headers_post["Referer"] = ref
            r = session.post(url, data=data, headers=headers_post, timeout=30, allow_redirects=True)
            dump(f"{label}.html", getattr(r, "text", ""), r.url, r.status_code)
            # Follow meta/JS redirect if present
            html = getattr(r, "text", "")
            red = _extract_meta_js_redirect(html or "")
            if red:
                r2 = session.get(_abs_url(base, red), headers=headers, timeout=30, allow_redirects=True)
                dump(f"{label}_redirect.html", getattr(r2, "text", ""), r2.url, r2.status_code)
                return r2
            return r
        except Exception as e:
            dump(f"{label}_exception.html", f"Exception during POST {url}\n{e}", url, None)
            raise

    def looks_like_worklist(html: str) -> bool:
        if not html:
            return False
        if "Worklist" in html:
            return True
        # Some pages may not include the word in title, check table header
        return "Study Requested" in html and "Report Out Time" not in html

    # A) Try Worklist directly (both casings) in case we already have a valid session
    for i, wp in enumerate(WORKLIST_PATHS, start=1):
        wl_url = _abs_url(base, wp)
        r = safe_get(wl_url, f"debug_wl_attempt_{i}")
        html = getattr(r, "text", "") or ""
        if looks_like_worklist(html):
            return base, html

    # B) Try multiple Index.aspx variants (with/without reporttype=1)
    r = None
    html = ""
    for i, ic in enumerate(INDEX_CANDIDATES, start=1):
        idx_url = _abs_url(base, ic)
        r_try = safe_get(idx_url, f"debug_index_{i}")
        html_try = getattr(r_try, "text", "") or ""
        if html_try:
            r = r_try
            html = html_try
            soup_try = BeautifulSoup(html_try, "html.parser")
            if ("Logout" in html_try or "Logged In:" in html_try) or _find_login_form(soup_try):
                break
    if not r:
        raise RuntimeError("Failed to fetch any Index.aspx variant")

    # Already logged in?
    if ("Logout" in html or "Logged In:" in html) and looks_like_worklist(html):
        r2 = safe_get(_abs_url(base, WORKLIST_PATHS[0]), "debug_wl_via_index")
        return base, getattr(r2, "text", "") or ""

    soup = BeautifulSoup(html, "html.parser")
    form = _find_login_form(soup)

    # C) Probe specific login endpoints if no form on Index.aspx variants
    if not form:
        for cand in LOGIN_CANDIDATES:
            r2 = safe_get(_abs_url(base, cand), f"debug_login_page_{cand.replace('/', '_')}")
            html2 = getattr(r2, "text", "") or ""
            soup2 = BeautifulSoup(html2, "html.parser")
            form = _find_login_form(soup2)
            if form:
                html = html2
                r = r2
                break

    if not form:
        dump("debug_no_login_form.html", html, getattr(r, "url", ""), getattr(r, "status_code", None))
        raise RuntimeError("No login form found")

    # D) POST credentials (post back to form action if present, otherwise to the same page URL we fetched)
    payload = _build_form_payload(form, username, password)
    action = (form.get("action") or "").strip()
    post_url = _abs_url(base, action) if action else getattr(r, "url", _abs_url(base, INDEX_CANDIDATES[0]))
    r3 = safe_post(post_url, payload, getattr(r, "url", post_url), "debug_after_login_post")

    # E) Fetch Worklist finally (try both casings)
    for i, wp in enumerate(WORKLIST_PATHS, start=1):
        wl_url = _abs_url(base, wp)
        r4 = safe_get(wl_url, f"debug_wl_final_{i}")
        html_wl = getattr(r4, "text", "") or ""
        if looks_like_worklist(html_wl):
            return base, html_wl

    # Try to discover a Worklist link from the current page as a last resort
    try:
        soup_post = BeautifulSoup(getattr(r3, "text", "") or "", "html.parser")
        for a in soup_post.find_all("a", href=True):
            if "Worklist" in a.get_text(" ", strip=True) or "Worklist" in a["href"]:
                wl_url = _abs_url(base, a["href"])
                r5 = safe_get(wl_url, "debug_wl_from_link")
                html_wl = getattr(r5, "text", "") or ""
                if looks_like_worklist(html_wl):
                    return base, html_wl
    except Exception as _:
        pass

    # If we got here, we didn't see Worklist even after POST
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
        inc = 0
        for p in parts:
            # Strict word boundaries to avoid matching CTA, MRN, etc.
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
