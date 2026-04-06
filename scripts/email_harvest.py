#!/usr/bin/env python3
"""
Email Harvester — SONA-adaptive live website scraper.

Fetches actual contact/about/team pages from business domains,
extracts real emails via regex. No port 25 needed.

SONA rate learning:
  - Starts at 0.5s delay per domain
  - Speeds up 3% per success
  - Backs off 2x on 429/403, 1.3x on 5xx/timeout
  - Separate rate state per TLD group (.ca, .com, other)

Usage:
    python3 email_harvest.py [--limit 5000] [--workers 20] [--db /opt/cangraph/canada_b2b.db]
"""
import argparse
import json
import os
import random
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from urllib.parse import urlparse

import requests

# ---------- Config ----------
DEFAULT_DB = "/opt/cangraph/canada_b2b.db"
_BASE = Path(os.environ.get("CANGRAPH_DIR", "/opt/cangraph"))
STATE_FILE = _BASE / "data" / "email_sona.json"
LOG_FILE = _BASE / "logs" / "email_harvest.log"

# Pages to check for emails (in priority order)
CONTACT_PATHS = [
    "/contact", "/contact-us", "/contactez-nous",
    "/about", "/about-us", "/a-propos",
    "/team", "/our-team", "/equipe",
    "/", # homepage often has footer email
]

# Email regex
EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# Phone regex (Canadian/NANP)
PHONE_RE = re.compile(
    r'(?:\+?1[\s.-]?)?'
    r'(?:\(?[2-9]\d{2}\)?[\s.-]?)'
    r'[2-9]\d{2}[\s.-]?\d{4}'
)

# Junk emails to skip
JUNK_PATTERNS = {
    "example.com", "example.org", "test.com", "sentry.io",
    "wixpress.com", "wordpress.com", "squarespace.com",
    "shopify.com", "godaddy.com", "googleapis.com",
    "cloudflare.com", "w3.org", "schema.org",
    "gravatar.com", "jquery.com", "bootstrap.com",
    "fontawesome.com", "google.com", "facebook.com",
    "twitter.com", "instagram.com", "linkedin.com",
    "youtube.com", "pinterest.com", "tiktok.com",
}

JUNK_PREFIXES = {"noreply", "no-reply", "donotreply", "mailer-daemon", "postmaster"}

# ISP/free email domains (deprioritize but still capture)
FREE_MAIL = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "live.com", "aol.com", "icloud.com", "msn.com",
}

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
]

print_lock = Lock()
db_lock = Lock()
stats = {"domains": 0, "pages_fetched": 0, "emails": 0, "phones": 0,
         "errors": 0, "skipped": 0, "catch_all": 0}
stats_lock = Lock()


# ---------- SONA ----------
DEFAULT_STATE = {
    "ca":    {"delay": 0.5, "successes": 0, "failures": 0, "backoff": 1.0},
    "com":   {"delay": 0.5, "successes": 0, "failures": 0, "backoff": 1.0},
    "other": {"delay": 0.8, "successes": 0, "failures": 0, "backoff": 1.0},
}


def load_state():
    if STATE_FILE.exists():
        try:
            st = json.loads(STATE_FILE.read_text())
            for k, v in DEFAULT_STATE.items():
                st.setdefault(k, dict(v))
            return st
        except Exception:
            pass
    return json.loads(json.dumps(DEFAULT_STATE))


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def tld_group(domain):
    if domain.endswith(".ca"):
        return "ca"
    elif domain.endswith(".com"):
        return "com"
    return "other"


def sona_learn(state, domain, success, status_code=None):
    grp = tld_group(domain)
    s = state[grp]
    if success:
        s["successes"] += 1
        s["delay"] = max(0.1, s["delay"] * 0.97)
        s["backoff"] = max(1.0, s["backoff"] * 0.9)
    else:
        s["failures"] += 1
        if status_code in (429, 403):
            s["backoff"] = min(30.0, s["backoff"] * 2.0)
            s["delay"] = min(10.0, s["delay"] * 1.5)
        else:
            s["delay"] = min(5.0, s["delay"] * 1.3)


def sona_sleep(state, domain):
    grp = tld_group(domain)
    s = state[grp]
    base = s["delay"] * s["backoff"]
    jitter = random.uniform(0, base * 0.2)
    time.sleep(base + jitter)


# ---------- Extraction ----------
def is_junk_email(email):
    email_lower = email.lower()
    local, _, domain = email_lower.partition("@")
    if domain in JUNK_PATTERNS:
        return True
    if local in JUNK_PREFIXES:
        return True
    if domain.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js")):
        return True
    if len(local) < 2 or len(domain) < 4:
        return True
    if ".." in email or email.startswith(".") or email.endswith("."):
        return True
    return False


def extract_contacts(html, source_domain):
    """Extract emails and phones from HTML."""
    emails = set()
    phones = set()

    for m in EMAIL_RE.finditer(html):
        email = m.group(0).lower().rstrip(".")
        if not is_junk_email(email):
            emails.add(email)

    for m in PHONE_RE.finditer(html):
        phone = re.sub(r'[\s.()\-]', '', m.group(0))
        if phone.startswith("+1"):
            phone = phone[2:]
        elif phone.startswith("1") and len(phone) == 11:
            phone = phone[1:]
        if len(phone) == 10:
            phones.add(phone)

    return emails, phones


def fetch_page(session, url, timeout=8):
    """Fetch a single page, return (status, html) or (status, None)."""
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True,
                        headers={"Accept": "text/html"})
        if r.status_code == 200 and "text/html" in r.headers.get("content-type", ""):
            return r.status_code, r.text[:100_000]  # cap at 100KB
        return r.status_code, None
    except requests.exceptions.Timeout:
        return 408, None
    except requests.exceptions.ConnectionError:
        return 0, None
    except Exception:
        return -1, None


def harvest_domain(domain, state, session):
    """Harvest emails/phones from a single domain across contact pages."""
    all_emails = set()
    all_phones = set()
    pages_ok = 0
    last_status = 0

    base = f"https://{domain}"

    for path in CONTACT_PATHS:
        url = base + path
        sona_sleep(state, domain)
        status, html = fetch_page(session, url)
        last_status = status

        if status == 200 and html:
            pages_ok += 1
            emails, phones = extract_contacts(html, domain)
            all_emails.update(emails)
            all_phones.update(phones)
            sona_learn(state, domain, True)

            # If we already found email on this domain, skip remaining paths
            if all_emails:
                break
        elif status in (429, 403):
            sona_learn(state, domain, False, status)
            break  # Don't hammer this domain
        elif status in (0, 408, -1):
            sona_learn(state, domain, False, status)
            break  # Domain unreachable
        else:
            sona_learn(state, domain, False, status)
            # Try next path

    # Also try http:// if https failed
    if pages_ok == 0 and last_status in (0, 408, -1):
        url = f"http://{domain}/"
        sona_sleep(state, domain)
        status, html = fetch_page(session, url)
        if status == 200 and html:
            pages_ok += 1
            emails, phones = extract_contacts(html, domain)
            all_emails.update(emails)
            all_phones.update(phones)
            sona_learn(state, domain, True)
        else:
            sona_learn(state, domain, False, status)

    return {
        "domain": domain,
        "emails": list(all_emails),
        "phones": list(all_phones),
        "pages_fetched": pages_ok,
        "last_status": last_status,
    }


# ---------- DB ----------
def ensure_tables(conn):
    conn.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS harvested_emails (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        business_id INTEGER,
        domain TEXT,
        email TEXT,
        phone TEXT,
        source_page TEXT,
        is_primary INTEGER DEFAULT 0,
        harvested_at TEXT DEFAULT (datetime('now')),
        UNIQUE(domain, email)
    );
    CREATE INDEX IF NOT EXISTS idx_he_domain ON harvested_emails(domain);
    CREATE INDEX IF NOT EXISTS idx_he_email ON harvested_emails(email);
    CREATE INDEX IF NOT EXISTS idx_he_biz ON harvested_emails(business_id);
    """)
    conn.commit()


def store_results(conn, biz_id, result):
    """Store harvested emails and phones."""
    stored = 0
    domain = result["domain"]

    for email in result["emails"]:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO harvested_emails (business_id, domain, email, harvested_at) "
                "VALUES (?, ?, ?, ?)",
                (biz_id, domain, email, datetime.now(timezone.utc).isoformat()))
            stored += 1
        except Exception:
            pass

    for phone in result["phones"]:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO harvested_emails (business_id, domain, phone, harvested_at) "
                "VALUES (?, ?, ?, ?)",
                (biz_id, domain, phone, datetime.now(timezone.utc).isoformat()))
            stored += 1
        except Exception:
            pass

    # Update businesses table with first email/phone found
    if result["emails"]:
        primary = sorted(result["emails"])[0]
        conn.execute(
            "UPDATE businesses SET email = ? WHERE id = ? AND (email IS NULL OR email = '')",
            (primary, biz_id))
    if result["phones"]:
        primary = sorted(result["phones"])[0]
        conn.execute(
            "UPDATE businesses SET phone = ? WHERE id = ? AND (phone IS NULL OR phone = '')",
            (primary, biz_id))

    return stored


def log(msg):
    with print_lock:
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        print(line, flush=True)


# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--limit", type=int, default=5000,
                    help="Max domains to process")
    ap.add_argument("--workers", type=int, default=20,
                    help="Concurrent fetch workers")
    ap.add_argument("--offset", type=int, default=0,
                    help="Skip first N domains (for resume)")
    args = ap.parse_args()

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db, timeout=30)
    ensure_tables(conn)

    # Get businesses with real domains (not slugified)
    rows = conn.execute("""
        SELECT b.id, b.domain FROM businesses b
        WHERE b.domain IS NOT NULL
          AND b.domain != ''
          AND b.domain LIKE '%.%'
          AND b.domain NOT LIKE '%..%'
          AND length(b.domain) > 4
          AND b.domain NOT IN (SELECT domain FROM harvested_emails)
        ORDER BY
            CASE WHEN b.email IS NULL OR b.email = '' THEN 0 ELSE 1 END,
            b.confidence DESC
        LIMIT ? OFFSET ?
    """, (args.limit, args.offset)).fetchall()

    log(f"Loaded {len(rows)} businesses with domains to harvest")
    if not rows:
        log("Nothing to do")
        return

    state = load_state()
    session = requests.Session()
    session.headers["User-Agent"] = random.choice(UA_POOL)

    total_emails = 0
    total_phones = 0
    total_pages = 0
    processed = 0
    batch = []
    batch_size = 50
    start_time = time.time()

    log(f"SONA starting delays: {json.dumps({k: round(v['delay'], 2) for k, v in state.items()})}")
    log(f"Workers: {args.workers}")

    def process_one(biz_id, domain):
        s = requests.Session()
        s.headers["User-Agent"] = random.choice(UA_POOL)
        return biz_id, harvest_domain(domain, state, s)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for biz_id, domain in rows:
            f = pool.submit(process_one, biz_id, domain)
            futures[f] = (biz_id, domain)

        for future in as_completed(futures):
            biz_id, domain = futures[future]
            processed += 1

            try:
                _, result = future.result()
                ne = len(result["emails"])
                np = len(result["phones"])
                total_emails += ne
                total_phones += np
                total_pages += result["pages_fetched"]

                if ne > 0 or np > 0:
                    with db_lock:
                        store_results(conn, biz_id, result)
                        batch.append(1)
                        if len(batch) >= batch_size:
                            conn.commit()
                            batch.clear()

                # Log progress every 50 domains
                if processed % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    log(f"[{processed}/{len(rows)}] {total_emails} emails, "
                        f"{total_phones} phones, {total_pages} pages "
                        f"({rate:.1f} domains/s) "
                        f"delay={state['ca']['delay']:.2f}s")

                if ne > 0:
                    log(f"  {domain}: {result['emails'][:3]}")

            except Exception as e:
                log(f"  ERROR {domain}: {e}")

            # Save SONA state periodically
            if processed % 100 == 0:
                save_state(state)

    # Final commit
    conn.commit()
    save_state(state)
    conn.close()

    elapsed = time.time() - start_time
    log("")
    log("=" * 50)
    log(f"HARVEST COMPLETE")
    log(f"  Domains processed: {processed}")
    log(f"  Pages fetched:     {total_pages}")
    log(f"  Emails found:      {total_emails}")
    log(f"  Phones found:      {total_phones}")
    log(f"  Time:              {elapsed:.0f}s ({elapsed/60:.1f} min)")
    log(f"  Rate:              {processed/elapsed:.1f} domains/s")
    log(f"  SONA final: {json.dumps({k: round(v['delay'], 2) for k, v in state.items()})}")
    log("=" * 50)


if __name__ == "__main__":
    main()
