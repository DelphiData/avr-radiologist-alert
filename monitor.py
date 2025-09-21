#!/usr/bin/env python3
"""
AVR Radiologist Alerts - monitor.py (data-scored table detector + robust column inference)

- Logs into AVR (www host only), tries Index.aspx with/without reporttype=1.
- After login, tries known Worklist paths then auto-discovers via links/iframes (depth 3).
- Detects Worklist by scoring tables that contain multiple rows with CT/MR + date + time (header-agnostic).
- Excludes "Completed Studies".
- Infers Date, Time, Study columns by column-wise pattern frequency.
- Counts CT/MR items into 60–89, 90–119, 120+ buckets.
- Always writes debug artifacts: docs/debug_*.html, docs/last_page.html, docs/last_counts.csv, docs/debug_table_headers.json.
- Publishes status.json for scripts/send_telegram.py.

Env: AVR_USERNAME, AVR_PASSWORD, TIMEZONE (default America/New_York), FORCE_ALERT
"""

import os, re, csv, json, pytz, yaml, datetime as dt
from typing import Optional, Tuple, Dict, Any, List, Set
from collections import deque, Counter
from urllib.parse import urlsplit, urlunsplit, urljoin

import requests
from bs4 import BeautifulSoup

# ---------------- Config ----------------

BASE_URL = "https://www.avrteleris.com/AVR"

INDEX_CANDIDATES = [
    "Index.aspx?reporttype=1",
    "index.aspx?reporttype=1",
    "Index.aspx",
    "index.aspx",
]

WORKLIST_PATHS = [
    "Forms/Worklist/Worklist.aspx",
    "Forms/Worklist/worklist.aspx",
    "Worklist.aspx",
    "Forms/PrelimReport/Worklist.aspx",
    "Forms/Results/Worklist.aspx",
    "Forms/WorkList/WorkList.aspx",
    "Forms/WorkList/worklist.aspx",
    "Forms/Prelim/Worklist.aspx",
]

LOGIN_CANDIDATES = [
    "Index.aspx?reporttype=1",
    "index.aspx?reporttype=1",
    "Login.aspx",
    "login.aspx",
    "Forms/Login.aspx",
    "Account/Login.aspx",
    "Default.aspx",
]

DISCOVERY_KEYWORDS = ["worklist","work list","results","report","prelim","pending"]

DEFAULT_TZ = "America/New_York"
DEFAULT_THRESHOLD = 20
UA = "Mozilla/5.0 (X11; Linux x86_64) AVR Monitor"

# ---------------- Helpers ----------------

def env_truthy(name: str, default: str = "false") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1","true","yes","y","on")

def now_in_tz(tzname: str) -> dt.datetime:
    tz = pytz.timezone(tzname)
    return dt.datetime.now(tz)

def allowed_window(now_local: dt.datetime) -> bool:
    wd = now_local.weekday()
    t = now_local.time()
    def between(a: dt.time, b: dt.time) -> bool: return a <= t <= b
    if 0 <= wd <= 4:  # Mon–Fri 6p–11:59:59p
        return between(dt.time(18,0), dt.time(23,59,59))
    if wd == 5:      # Sat 4a–11:59:59p
        return between(dt.time(4,0), dt.time(23,59,59))
    return between(dt.time(0,0), dt.time(21,0))  # Sun until 9p

def read_yaml(path: str, default: Any) -> Any:
    try:
        with open(path,"r",encoding="utf-8") as f: return yaml.safe_load(f) or default
    except Exception: return default

def safe_write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,"w",encoding="utf-8") as f: f.write(text or "")

def safe_write_status(status: Dict[str,Any]):
    with open("status.json","w",encoding="utf-8") as f: json.dump(status,f,indent=2,ensure_ascii=False)

def _abs_url(base: str, path: str) -> str:
    if path.startswith(("http://","https://")): return path
    if path.startswith("/"):
        sp = urlsplit(base)
        return urlunsplit((sp.scheme, sp.netloc, path, "", ""))
    return f"{base.rstrip('/')}/{path.lstrip('/')}"

def _same_origin(base: str, url: str) -> bool:
    a, b = urlsplit(base), urlsplit(url)
    return (a.scheme, a.netloc) == (b.scheme, b.netloc)

def _extract_meta_js_redirect(html: str) -> Optional[str]:
    if not html: return None
    soup = BeautifulSoup(html, "html.parser")
    for m in soup.find_all("meta"):
        if m.get("http-equiv","").lower()=="refresh":
            mm = re.search(r"url=(.+)", m.get("content",""), re.I)
            if mm: return mm.group(1).strip().strip("'\"")
    mm = re.search(r"location\.(?:href|replace)\(['\"]([^'\"]+)['\"]\)", html, re.I)
    if mm: return mm.group(1).strip()
    mm = re.search(r"window\.location\s*=\s*['\"]([^'\"]+)['\"]", html, re.I)
    if mm: return mm.group(1).strip()
    return None

def _find_login_form(soup: BeautifulSoup):
    for form in soup.find_all("form"):
        if form.find("input", {"type":"password"}): return form
    return None

def _build_form_payload(form, username: str, password: str) -> Dict[str,str]:
    payload: Dict[str,str] = {}
    for inp in form.find_all("input"):
        n = inp.get("name"); 
        if n: payload[n] = inp.get("value","")
    def set_best(names, val):
        ok=False
        for cand in names:
            for k in list(payload.keys()):
                if k.lower()==cand.lower(): payload[k]=val; ok=True
        return ok
    user_set = set_best(["username","user","userid","login","txtusername","ctl00$maincontent$txtusername"], username)
    pass_set = set_best(["password","pwd","pass","txtpassword","ctl00$maincontent$txtpassword"], password)
    if not user_set:
        for inp in form.find_all("input"):
            if (inp.get("type") or "").lower() in ("text","email"):
                n=inp.get("name"); 
                if n: payload
