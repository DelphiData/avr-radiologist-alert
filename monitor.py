#!/usr/bin/env python3
"""
AVR Radiologist Alerts - monitor.py (worklist detector fixed for TD headers)

- Logs into AVR (www host only), tries Index.aspx with/without reporttype=1.
- After login, tries known Worklist paths then auto-discovers via links/iframes (depth 3).
- Detects Worklist even if the header row uses <td> cells (not only <th>).
- Counts CT/MR items into 60–89, 90–119, 120+ buckets.
- Always writes debug artifacts: docs/debug_*.html, docs/last_page.html, docs/last_counts.csv, docs/debug_table_headers.json.
- Publishes status.json for scripts/send_telegram.py.

Env: AVR_USERNAME, AVR_PASSWORD, TIMEZONE (default America/New_York), FORCE_ALERT
"""

import os, re, csv, json, pytz, yaml, datetime as dt
from typing import Optional, Tuple, Dict, Any, List, Set
from collections import deque
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
                if n: payload[n]=username; user_set=True; break
    if not pass_set:
        for inp in form.find_all("input"):
            if (inp.get("type") or "").lower()=="password":
                n=inp.get("name"); 
                if n: payload[n]=password; pass_set=True; break
    for inp in form.find_all("input"):
        if (inp.get("type") or "").lower() in ("submit","button"):
            n=inp.get("name")
            if n and n not in payload: payload[n]=inp.get("value","") or "Login"
    return payload

# ---------------- Discovery helpers ----------------

def looks_like_worklist(html: str) -> bool:
    if not html: return False
    # Exclude Completed Studies
    if "Completed Studies" in html or "Report Out Time" in html: return False
    # Strong signal:
    if "Study Requested" in html and "Minutes Since Request" in html:
        return True
    # Backup signals
    header_hits = ("Study Requested","Requested Time","Request Time","Modality","Study")
    return sum(1 for k in header_hits if k in html) >= 3

def collect_links_and_frames(base_page_url: str, html: str, base_origin: str) -> List[str]:
    urls: List[str] = []
    soup = BeautifulSoup(html or "", "html.parser")
    for a in soup.find_all("a", href=True):
        u = urljoin(base_page_url, a["href"].strip())
        if _same_origin(base_origin, u): urls.append(u)
    for fr in soup.find_all(["frame","iframe"]):
        src = (fr.get("src") or "").strip()
        if src:
            u = urljoin(base_page_url, src)
            if _same_origin(base_origin, u): urls.append(u)
    seen=set(); ordered=[]
    for u in urls:
        if u not in seen: seen.add(u); ordered.append(u)
    return ordered

def prioritize(urls: List[str]) -> List[str]:
    scored=[]
    for u in urls:
        low=u.lower()
        score=sum(1 for k in DISCOVERY_KEYWORDS if k in low)
        scored.append((score,u))
    scored.sort(key=lambda x:(-x[0], x[1]))
    return [u for _,u in scored]

# ---------------- Login + fetch ----------------

def try_login_and_fetch_worklist(session: requests.Session, username: str, password: str, dbg: bool = True) -> Tuple[str, str]:
    headers={"User-Agent": UA}
    base=BASE_URL

    def dump(name: str, html: str, url: str="", status: Optional[int]=None):
        if not dbg: return
        os.makedirs("docs", exist_ok=True)
        prefix = f"<!-- url={url} status={status} -->\n" if (url or status is not None) else ""
        safe_write_text(f"docs/{name}", prefix + (html or ""))

    def safe_get(url: str, label: str) -> requests.Response:
        try:
            r = session.get(url, headers=headers, timeout=30, allow_redirects=True)
            dump(f"{label}.html", getattr(r,"text",""), r.url, r.status_code)
            red=_extract_meta_js_redirect(getattr(r,"text","") or "")
            if red:
                r2=session.get(_abs_url(base, red), headers=headers, timeout=30, allow_redirects=True)
                dump(f"{label}_redirect.html", getattr(r2,"text",""), r2.url, r2.status_code)
                return r2
            return r
        except Exception as e:
            dump(f"{label}_exception.html", f"Exception during GET {url}\n{e}", url, None)
            raise

    def safe_post(url: str, data: Dict[str,str], ref: str, label: str) -> requests.Response:
        try:
            h=dict(headers); h["Referer"]=ref
            r=session.post(url, data=data, headers=h, timeout=30, allow_redirects=True)
            dump(f"{label}.html", getattr(r,"text",""), r.url, r.status_code)
            red=_extract_meta_js_redirect(getattr(r,"text","") or "")
            if red:
                r2=session.get(_abs_url(base, red), headers=headers, timeout=30, allow_redirects=True)
                dump(f"{label}_redirect.html", getattr(r2,"text",""), r2.url, r2.status_code)
                return r2
            return r
        except Exception as e:
            dump(f"{label}_exception.html", f"Exception during POST {url}\n{e}", url, None)
            raise

    # Try direct Worklist (already authenticated session?)
    for i,wp in enumerate(WORKLIST_PATHS,1):
        r=safe_get(_abs_url(base,wp), f"debug_wl_attempt_{i}")
        if looks_like_worklist(getattr(r,"text","") or ""): return base, getattr(r,"text","") or ""

    # Fetch index variants
    r=None; html=""
    for i, ic in enumerate(INDEX_CANDIDATES,1):
        r_try=safe_get(_abs_url(base,ic), f"debug_index_{i}")
        html_try=getattr(r_try,"text","") or ""
        if html_try:
            r=r_try; html=html_try
            soup_try=BeautifulSoup(html_try,"html.parser")
            if ("Logout" in html_try or "Logged In:" in html_try) or _find_login_form(soup_try): break
    if not r: raise RuntimeError("Failed to fetch any Index.aspx variant")

    # Possibly already logged in
    if ("Logout" in html or "Logged In:" in html) and looks_like_worklist(html):
        r2=safe_get(_abs_url(base, WORKLIST_PATHS[0]), "debug_wl_via_index")
        return base, getattr(r2,"text","") or ""

    soup=BeautifulSoup(html,"html.parser")
    form=_find_login_form(soup)

    # Probe known login pages if needed
    if not form:
        for cand in LOGIN_CANDIDATES:
            r2=safe_get(_abs_url(base,cand), f"debug_login_page_{cand.replace('/','_')}")
            html2=getattr(r2,"text","") or ""
            soup2=BeautifulSoup(html2,"html.parser")
            form=_find_login_form(soup2)
            if form: html=html2; r=r2; break

    if not form:
        dump("debug_no_login_form.html", html, getattr(r,"url",""), getattr(r,"status_code",None))
        raise RuntimeError("No login form found")

    payload=_build_form_payload(form, username, password)
    action=(form.get("action") or "").strip()
    post_url=_abs_url(base, action) if action else getattr(r,"url",_abs_url(base, INDEX_CANDIDATES[0]))
    r3=safe_post(post_url, payload, getattr(r,"url",post_url), "debug_after_login_post")

    # Try fixed Worklist after login
    for i, wp in enumerate(WORKLIST_PATHS,1):
        r4=safe_get(_abs_url(base,wp), f"debug_wl_final_{i}")
        html_wl=getattr(r4,"text","") or ""
        if looks_like_worklist(html_wl): return base, html_wl

    # Discovery crawl (depth 3, max 60 fetches)
    start_url=getattr(r3,"url",_abs_url(base, INDEX_CANDIDATES[0]))
    start_html=getattr(r3,"text","") or ""
    origin=BASE_URL

    q=deque(); seen:set[str]=set()
    for u in prioritize(collect_links_and_frames(start_url,start_html,origin)):
        if u.startswith(BASE_URL): q.append((u,1)); seen.add(u)
    max_depth=3; visit_count=0; max_visits=60

    while q and visit_count<max_visits:
        url, depth = q.popleft()
        visit_count += 1
        rX=safe_get(url, f"debug_discover_{visit_count:02d}")
        htmlX=getattr(rX,"text","") or ""
        if looks_like_worklist(htmlX): return base, htmlX
        if depth<max_depth:
            for u in prioritize(collect_links_and_frames(getattr(rX,"url",url), htmlX, origin)):
                if u not in seen and u.startswith(BASE_URL):
                    seen.add(u); q.append((u, depth+1))

    raise RuntimeError("Login POST ok but Worklist not reachable")

# ---------------- Parser ----------------

DATE_HEADERS = ["Study Requested","Requested Date","Request Date","Date Requested","Scheduled Date","Date"]
TIME_HEADERS = ["Requested Time","Request Time","Study Requested Time","Time"]
STUDY_HEADERS = ["Study Requested","Study","Procedure","Exam","Study Name","Study Description","Procedure Requested"]
MOD_HEADERS   = ["Modality","Study Modality","Mod"]

def _header_cells(row) -> List[str]:
    return [c.get_text(" ", strip=True) for c in row.find_all(["th","td"])]

def _table_header_candidates(t: BeautifulSoup) -> List[List[str]]:
    rows = t.find_all("tr")
    heads: List[List[str]] = []
    for i, tr in enumerate(rows[:3]):  # first few rows usually include header
        cells = _header_cells(tr)
        if not cells: continue
        joined = " ".join(cells)
        if ("Study Requested" in joined) or any(h in joined for h in ("Requested","Request","Modality","Study")):
            heads.append(cells)
    return heads

def _find_worklist_table(soup: BeautifulSoup) -> Tuple[Optional[BeautifulSoup], List[str]]:
    best=None; best_headers=[]
    for t in soup.find_all("table"):
        # Exclude obvious completed studies tables
        if "Report Out Time" in t.get_text(" ", strip=True) or "Completed Studies" in t.get_text(" ", strip=True):
            continue
        headersets = _table_header_candidates(t)
        for hdr in headersets:
            joined = " ".join(hdr)
            if "Study Requested" in joined:
                return t, hdr
            # score fallback: must at least mention Study and Time/Date
            score = 0
            if any(h in joined for h in STUDY_HEADERS): score += 1
            if any(h in joined for h in DATE_HEADERS+TIME_HEADERS): score += 1
            if score >= 2 and not best:
                best, best_headers = t, hdr
    return best, best_headers

def _pick_column_indexes(ths: List[str]) -> Tuple[int,int,int]:
    def idx_for(names):
        for i,h in enumerate(ths):
            for n in names:
                if n.lower() in h.lower(): return i
        return -1
    date_i = idx_for(DATE_HEADERS)
    time_i = idx_for(TIME_HEADERS)
    study_i= idx_for(STUDY_HEADERS)
    return date_i, time_i, study_i

def parse_worklist_counts(html: str, now_local: dt.datetime, tz) -> Tuple[Dict[str,int], Dict[str,Any]]:
    soup=BeautifulSoup(html,"html.parser")
    table, headers = _find_worklist_table(soup)

    counts={"60":0,"90":0,"120":0}
    rows_seen=0; considered=0; errors:List[str]=[]; included_rows:List[Dict[str,Any]]=[]

    # Save detected headers for debugging
    try:
        os.makedirs("docs", exist_ok=True)
        with open("docs/debug_table_headers.json","w",encoding="utf-8") as f:
            json.dump({"headers": headers}, f, indent=2)
    except Exception:
        pass

    if table is None:
        errors.append("worklist table not found")
        return counts, {"rows_seen":0,"ctmr_considered":0,"errors":errors,"included_rows":[]}

    # Determine columns
    if not headers:
        # If we didn't capture headers from first rows, try first row explicitly
        first = table.find("tr")
        headers = _header_cells(first) if first else []
    date_i, time_i, study_i = _pick_column_indexes(headers)

    def parse_req_dt(date_str: str, time_str: str):
        s = f"{date_str} {time_str}".strip()
        fmts = [
            "%b %d, %Y %H:%M:%S",  # Sep 21, 2025 13:45:12
            "%b %d, %Y %H:%M",     # Sep 21, 2025 13:45
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
        ]
        for fmt in fmts:
            try:
                return tz.localize(dt.datetime.strptime(s, fmt))
            except Exception:
                continue
        return None

    for tr in table.find_all("tr"):
        # skip header-like rows
        if tr.find("th"): 
            continue
        cells = tr.find_all("td")
        if not cells:
            continue

        # Heuristic: many data rows have at least 7 columns on the Worklist
        if len(cells) < 5:
            continue

        rows_seen += 1
        # Extract text
        td_texts = [c.get_text(" ", strip=True) for c in cells]

        # Study cell
        study_text = td_texts[study_i] if (0 <= study_i < len(td_texts)) else ""
        if not study_text:
            # choose the longest cell containing CT/MR hints
            study_text = max(td_texts, key=lambda s: (("CT" in s.upper()) or ("MR" in s.upper()), len(s)), default="")

        # Date/time
        date_text = td_texts[date_i] if (0 <= date_i < len(td_texts)) else ""
        time_text = td_texts[time_i] if (0 <= time_i < len(td_texts)) else ""
        if not (date_text and time_text):
            # Find a date/time pair by pattern
            for s in td_texts:
                if not date_text and re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}", s):
                    date_text = s
                if not time_text and re.match(r"\d{1,2}:\d{2}(:\d{2})?$", s):
                    time_text = s

        # Count CT/MR tokens
        upper=study_text.upper()
        parts=[p.strip() for p in re.split(r"[;,/]|(?:\bAND\b)", upper) if p.strip()]
        inc=0
        for p in parts:
            inc += len(re.findall(r"\bCT\b", p))
            inc += len(re.findall(r"\bMRI?\b", p))  # MR or MRI
        if inc==0:
            continue

        req_dt=parse_req_dt(date_text, time_text) if (date_text and time_text) else None
        if not req_dt:
            # If we can’t age it, skip
            continue

        minutes=int((now_local - req_dt).total_seconds()//60)
        if minutes < 60:
            continue

        bucket="120" if minutes>=120 else ("90" if minutes>=90 else "60")
        counts[bucket]+=inc
        considered+=inc
        included_rows.append({"bucket":bucket,"age_min":minutes,"study_cell":study_text,"ct_mr_count_in_row":inc})

    debug={"rows_seen":rows_seen,"ctmr_considered":considered,"errors":errors,"included_rows":included_rows}
    return counts, debug

# ---------------- Main ----------------

def main():
    username=os.getenv("AVR_USERNAME","").strip()
    password=os.getenv("AVR_PASSWORD","").strip()
    tzname=os.getenv("TIMEZONE", DEFAULT_TZ)
    force_alert=env_truthy("FORCE_ALERT","false")

    tz=pytz.timezone(tzname)
    now_local=now_in_tz(tzname)

    print(f"[INFO] Timezone: {tzname}, now={now_local:%Y-%m-%d %H:%M:%S}")
    allowed=allowed_window(now_local)
    print(f"[INFO] Allowed window now: {allowed}")

    cfg=read_yaml("config.yml",{}) or {}
    threshold=int(cfg.get("threshold_total", DEFAULT_THRESHOLD))

    contacts=read_yaml("contacts.yml",[]) or []
    contact_names=[c.get("name") for c in contacts if isinstance(c,dict) and c.get("name")]
    if not contact_names: contact_names=["Reed","Bargo","Croce"]

    errors:List[str]=[]
    html=""; base_used=BASE_URL

    try:
        with requests.Session() as s:
            s.headers.update({"User-Agent": UA})
            base_used, html = try_login_and_fetch_worklist(s, username, password, dbg=True)
    except Exception as e:
        err=f"fetch: {type(e).__name__}: {e}"
        print(f"[ERROR] {err}")
        errors.append(err)

    counts={"60":0,"90":0,"120":0}
    debug={"rows_seen":0,"ctmr_considered":0,"errors":[], "included_rows":[]}
    if html:
        counts, debug = parse_worklist_counts(html, now_local, tz)
    else:
        debug["errors"].append("no HTML fetched")

    total=counts["60"]+counts["90"]+counts["120"]
    print(f"[INFO] Counts: 60={counts['60']} 90={counts['90']} 120={counts['120']} total={total} threshold={threshold}")
    print(f"[INFO] Force alert: {force_alert}")

    try:
        os.makedirs("docs", exist_ok=True)
        if html: safe_write_text("docs/last_page.html", html)
        with open("docs/last_counts.csv","w",newline="",encoding="utf-8") as f:
            w=csv.writer(f); w.writerow(["bucket","age_min","study_cell","ct_mr_count_in_row"])
            for r in debug.get("included_rows",[]): w.writerow([r.get("bucket",""),r.get("age_min",""),r.get("study_cell",""),r.get("ct_mr_count_in_row","")])
    except Exception as e:
        print(f"[WARN] Failed to save debug snapshots: {e}")

    alert_ok = (total>=threshold) and allowed
    if force_alert: alert_ok=True

    message = f"CT/MR backlog: 60m={counts['60']}, 90m={counts['90']}, 120m={counts['120']} (total={total} {'>=' if total>=threshold else '<'} {threshold}). Notifying: {', '.join(contact_names)}"

    status={
        "timestamp": now_local.strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": tzname,
        "allowed_window": allowed,
        "counts": counts,
        "total_ctmr_60_90_120": total,
        "alert_triggered": alert_ok,
        "message": message,
        "config": {"threshold_total": threshold, "allowed_window_desc":"Mon–Fri 6pm–11pm, Sat 4am–Sun 9pm"},
        "contacts_total": len(contact_names),
        "notification": {"telegram":{"recipients":[],"results":[],"time_utc":""}},
        "scrape": {"rows_seen": debug.get("rows_seen"), "ctmr_considered": debug.get("ctmr_considered"), "errors": (errors+debug.get('errors',[])), "base_url": base_used},
    }

    safe_write_status(status)
    print("[INFO] Wrote status.json")

if __name__=="__main__":
    main()
