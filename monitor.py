import os
import re
import json
import time
import yaml
import smtplib
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional

from tenacity import retry, stop_after_attempt, wait_fixed

from twilio.rest import Client
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

ROOT = Path(__file__).parent
DOCS = ROOT / "docs"
DOCS.mkdir(exist_ok=True)

CONFIG_PATH = ROOT / "config.yml"
CONTACTS_PATH = ROOT / "contacts.yml"
STATUS_PATH = DOCS / "status.json"
SNAPSHOT_HTML = DOCS / "last_page.html"
SNAPSHOT_PNG = DOCS / "last_screenshot.png"
COLOR_SAMPLES = DOCS / "color_samples.json"

def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def now_local(tz_name: str) -> datetime:
    return datetime.now(ZoneInfo(tz_name))

def in_window(now_: datetime, windows: List[Dict[str, Any]]) -> bool:
    # days as Mon, Tue, Wed, Thu, Fri, Sat, Sun
    day = now_.strftime("%a")
    hhmm = now_.strftime("%H:%M")
    for w in windows:
        if day in w.get("days", []):
            start = w.get("start", "00:00")
            end = w.get("end", "23:59")
            if start <= hhmm <= end:
                return True
    return False

def normalize_phone(p: str) -> str:
    p = p.strip()
    if p.startswith("+"):
        return p
    digits = re.sub(r"\D", "", p)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return p  # as-is fallback

def build_message(counts: Dict[str, int], total: int, when: datetime, tz: str) -> str:
    return (
        f"AVR Alert: CT/MR counts: 60m={counts.get('60',0)}, "
        f"90m={counts.get('90',0)}, 120m={counts.get('120',0)}; "
        f"total={total} (>= threshold). Time {when.strftime('%Y-%m-%d %H:%M')} {tz}. "
        f"https://avrteleris.com/AVR/Index.aspx"
    )

def carrier_email(phone: str, carrier: str) -> Optional[str]:
    # Common US gateways
    domains = {
        "att": "txt.att.net",
        "at&t": "txt.att.net",
        "verizon": "vtext.com",
        "tmobile": "tmomail.net",
        "t-mobile": "tmomail.net",
        "sprint": "messaging.sprintpcs.com",
        "uscellular": "email.uscc.net",
        "us-cellular": "email.uscc.net",
        "xfinity": "vtext.com",
        "googlefi": "msg.fi.google.com",
        "cricket": "sms.cricketwireless.net",
    }
    d = domains.get(carrier.lower().strip(), None) if carrier else None
    if not d:
        return None
    digits = re.sub(r"\D", "", phone)
    if digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"{digits}@{d}"

def send_via_twilio(text: str, contacts: List[Dict[str, Any]]) -> Dict[str, Any]:
    sid = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")
    from_number = os.getenv("TWILIO_FROM_NUMBER")

    if not (sid and token and from_number):
        return {"ok": False, "error": "Missing Twilio env vars", "sent": 0}

    client = Client(sid, token)
    sent = 0
    errors = []
    for c in contacts:
        to = normalize_phone(c.get("phone", ""))
        try:
            client.messages.create(body=text, from_=from_number, to=to)
            sent += 1
        except Exception as e:
            errors.append({"to": to, "error": str(e)})
    return {"ok": errors == [], "sent": sent, "errors": errors}

def send_via_email_to_sms(text: str, contacts: List[Dict[str, Any]]) -> Dict[str, Any]:
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT") or "587")
    user = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")
    from_addr = os.getenv("SMTP_FROM")

    if not (host and port and user and password and from_addr):
        return {"ok": False, "error": "Missing SMTP env vars", "sent": 0}

    sent = 0
    errors = []
    try:
        with smtplib.SMTP(host, port, timeout=20) as server:
            server.starttls()
            server.login(user, password)
            for c in contacts:
                email_addr = carrier_email(c.get("phone",""), c.get("carrier",""))
                if not email_addr:
                    errors.append({"contact": c, "error": "No email gateway"})
                    continue
                msg = MIMEText(text)
                msg["Subject"] = "AVR Alert"
                msg["From"] = from_addr
                msg["To"] = email_addr
                try:
                    server.sendmail(from_addr, [email_addr], msg.as_string())
                    sent += 1
                except Exception as e:
                    errors.append({"to": email_addr, "error": str(e)})
    except Exception as e:
        return {"ok": False, "error": str(e), "sent": sent, "errors": errors}
    return {"ok": errors == [], "sent": sent, "errors": errors}

def parse_minutes_from_text(s: str) -> Optional[int]:
    s = s.lower()
    # e.g., "45 min", "1 hr 15 min"
    m = re.search(r"(\d+)\s*(?:hours?|hrs?|h)\s*(\d+)\s*(?:mins?|m)\b", s)
    if m:
        return int(m.group(1))*60 + int(m.group(2))
    m = re.search(r"(\d+)\s*(?:hours?|hrs?|h)\b", s)
    if m:
        return int(m.group(1))*60
    m = re.search(r"(\d+)\s*(?:mins?|m)\b", s)
    if m:
        return int(m.group(1))
    return None

def bucket_for_minutes(mins: int, buckets: List[int]) -> Optional[str]:
    # Put into the smallest bucket that is >= mins
    for b in sorted(buckets):
        if mins <= b:
            return str(b)
    return None

def find_table_locator(page):
    # Heuristic: prefer tables with many rows
    tables = page.locator("table")
    n = tables.count()
    best = None
    best_rows = 0
    for i in range(n):
        t = tables.nth(i)
        rows = t.locator("tr").count()
        if rows > best_rows:
            best = t
            best_rows = rows
    return best

def extract_row_bgcolor(page, row_element_handle) -> Optional[str]:
    try:
        return page.evaluate("(el) => getComputedStyle(el).backgroundColor", row_element_handle)
    except Exception:
        return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def run_scrape(config: Dict[str, Any]) -> Dict[str, Any]:
    site = config["site"]
    login_url = site["login_url"]
    selectors = site["selectors"]
    buckets = config["buckets"]
    modalities = set(m.upper() for m in config.get("modalities", []))
    color_map = config.get("color_map", {})

    results = {
        "rows_seen": 0,
        "ctmr_considered": 0,
        "bucket_counts": {str(b): 0 for b in buckets},
        "color_samples": {},
        "errors": [],
        "sample_rows": []
    }

    headless = os.getenv("HEADLESS", "true").lower() != "false"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()
        try:
            page.goto(login_url, wait_until="domcontentloaded", timeout=30000)
        except PlaywrightTimeoutError:
            results["errors"].append("Timeout loading login page")
            browser.close()
            return results

        # Try to fill username/password
        try:
            page.locator(selectors["username"]).first.fill(os.getenv("AVR_USERNAME", ""))
            page.locator(selectors["password"]).first.fill(os.getenv("AVR_PASSWORD", ""))
        except Exception:
            # Fallback: heuristic inputs
            inputs = page.locator("input[type='text']")
            if inputs.count() > 0:
                inputs.first.fill(os.getenv("AVR_USERNAME",""))
            pw = page.locator("input[type='password']")
            if pw.count() > 0:
                pw.first.fill(os.getenv("AVR_PASSWORD",""))

        # Click submit
        try:
            page.locator(selectors["submit"]).first.click()
        except Exception:
            # Try common submit buttons
            try:
                page.get_by_role("button", name=re.compile("login", re.I)).click()
            except Exception:
                try:
                    page.locator("input[type='submit']").first.click()
                except Exception as e:
                    results["errors"].append(f"Could not submit login: {e}")

        # Wait for potential redirect/content
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        # Snapshot for discovery/tuning
        try:
            SNAPSHOT_PNG.unlink(missing_ok=True)
            SNAPSHOT_HTML.unlink(missing_ok=True)
            page.screenshot(path=str(SNAPSHOT_PNG), full_page=True)
            html = page.content()
            SNAPSHOT_HTML.write_text(html, encoding="utf-8")
        except Exception as e:
            results["errors"].append(f"Snapshot failed: {e}")

        table = find_table_locator(page)
        if not table:
            results["errors"].append("No table found after login")
            browser.close()
            return results

        rows = table.locator("tr")
        row_count = rows.count()
        results["rows_seen"] = row_count

        # Identify headers if present
        headers = []
        try:
            headers = [h.inner_text().strip() for h in table.locator("th").all()]
        except Exception:
            headers = []

        def get_cell_texts(row):
            try:
                tds = row.locator("td")
                return [tds.nth(i).inner_text().strip() for i in range(tds.count())]
            except Exception:
                return []

        def modality_from_texts(txts: List[str]) -> Optional[str]:
            combo = " | ".join(txts).upper()
            for m in modalities:
                # match whole word CT / MR / MRI
                if re.search(rf"\b{re.escape(m)}\b", combo):
                    return m
            return None

        def minutes_from_texts(txts: List[str]) -> Optional[int]:
            combo = " | ".join(txts)
            mins = parse_minutes_from_text(combo)
            if mins is not None:
                return mins
            # Try explicit datetime stamps like 09/08/2025 19:45 or 9/8/2025 7:45 PM
            patterns = [
                "%m/%d/%Y %H:%M",
                "%m/%d/%Y %I:%M %p",
                "%m/%d/%y %H:%M",
                "%m/%d/%y %I:%M %p",
            ]
            tz = ZoneInfo(config["timezone"])
            for t in txts:
                for fmt in patterns:
                    try:
                        dt = datetime.strptime(t.strip(), fmt)
                        # Assume local time
                        dt = dt.replace(tzinfo=tz)
                        delta = datetime.now(tz) - dt
                        return max(0, int(delta.total_seconds() // 60))
                    except Exception:
                        continue
            return None

        # Iterate rows
        for i in range(row_count):
            row = rows.nth(i)
            cells = get_cell_texts(row)
            if not cells:
                continue

            mod = modality_from_texts(cells)
            if not mod:
                continue  # skip non-CT/MR rows

            results["ctmr_considered"] += 1

            mins = minutes_from_texts(cells)
            bucket = None

            if mins is not None:
                bucket = bucket_for_minutes(mins, buckets)
            if bucket is None:
                # Try color mapping if mins not available
                try:
                    handle = row.element_handle()
                    color = extract_row_bgcolor(page, handle) if handle else None
                    if color:
                        results["color_samples"][color] = results["color_samples"].get(color, 0) + 1
                        mapped = color_map.get(color)
                        if mapped:
                            bucket = str(mapped)
                except Exception:
                    pass

            if bucket and bucket in results["bucket_counts"]:
                results["bucket_counts"][bucket] += 1

            # save a few sample rows
            if len(results["sample_rows"]) < 10:
                results["sample_rows"].append({
                    "modality": mod,
                    "texts": cells[:8],  # truncate
                    "mins": mins,
                    "bucket": bucket
                })

        # Record color samples for later tuning
        try:
            COLOR_SAMPLES.write_text(json.dumps(results["color_samples"], indent=2), encoding="utf-8")
        except Exception:
            pass

        browser.close()
        return results

def save_status(status: Dict[str, Any]):
    STATUS_PATH.write_text(json.dumps(status, indent=2), encoding="utf-8")

def main():
    config = load_yaml(CONFIG_PATH)
    contacts = load_yaml(CONTACTS_PATH).get("radiologists", [])
    tz = config["timezone"]
    now = now_local(tz)

    # Gate by time windows
    allowed = in_window(now, config.get("windows", []))

    scrape_data = {}
    alert_sent = False
    alert_errors = None
    message = ""
    try:
        scrape_data = run_scrape(config)
    except Exception as e:
        scrape_data = {"errors": [f"Scrape fatal error: {e}"]}

    counts = {str(b): 0 for b in config["buckets"]}
    counts.update(scrape_data.get("bucket_counts", {}))
    total = sum(counts.get(str(b), 0) for b in config["buckets"])

    if allowed and total >= config["threshold"]["total_min"]:
        message = build_message(counts, total, now, tz)
        # Prefer Twilio; fallback to email-to-SMS if configured
        result = send_via_twilio(message, contacts)
        if not result.get("ok"):
            # Try email-to-SMS only if SMTP is configured
            fallback = send_via_email_to_sms(message, contacts)
            alert_sent = fallback.get("ok", False)
            alert_errors = {"twilio": result, "email_to_sms": fallback}
        else:
            alert_sent = True
            alert_errors = None
    else:
        alert_sent = False

    status = {
        "timestamp": now.isoformat(),
        "timezone": tz,
        "allowed_window": allowed,
        "threshold": config["threshold"],
        "buckets": config["buckets"],
        "counts": counts,
        "total_ctmr_60_90_120": total,
        "alert_triggered": alert_sent,
        "message": message if alert_sent else "",
        "scrape": {
            "rows_seen": scrape_data.get("rows_seen"),
            "ctmr_considered": scrape_data.get("ctmr_considered"),
            "errors": scrape_data.get("errors", []),
            "color_samples_count": scrape_data.get("color_samples", {}),
            "sample_rows": scrape_data.get("sample_rows", []),
        },
    }

    save_status(status)
    print(json.dumps(status, indent=2))

if __name__ == "__main__":
    main()
