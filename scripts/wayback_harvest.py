#!/usr/bin/env python3
"""
Wayback Machine CDX Harvester for Canadian Domains.

For each domain:
  1. Query CDX for archived contact/about/team pages
  2. Fetch up to N snapshots
  3. Extract emails & phones with regex
  4. Store in `wayback_contacts` table

Prioritizes businesses with no current email/phone (likely dead sites).

Usage:
  python3 wayback_harvest.py --db /opt/cangraph/canada_b2b.db --limit 100
  python3 wayback_harvest.py --db /opt/cangraph/canada_b2b.db --limit 500 --workers 4
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
import threading
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# --- Regex patterns --------------------------------------------------------
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
# Canadian / NANP phone: (416) 555-1234, 416-555-1234, +1 416 555 1234, 4165551234
PHONE_RE = re.compile(
    r"(?:\+?1[\s\-.]?)?\(?([2-9]\d{2})\)?[\s\-.]?([2-9]\d{2})[\s\-.]?(\d{4})\b"
)

# Filter out obvious junk emails
BAD_EMAIL_SUFFIXES = (
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
    ".css", ".js", ".pdf", ".zip",
)
BAD_EMAIL_PREFIXES = ("example@", "user@", "name@", "email@", "your@", "test@", "no-reply@", "noreply@")

CDX_API = "https://web.archive.org/cdx/search/cdx"
WAYBACK_BASE = "https://web.archive.org/web"
USER_AGENT = "Mozilla/5.0 (compatible; CangraphWaybackHarvester/1.0; +research)"

# --- Rate limiter (global token bucket-ish, thread-safe) -------------------
class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.min_interval = 1.0 / rate_per_sec
        self.lock = threading.Lock()
        self.next_slot = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            wait = self.next_slot - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self.next_slot = max(now, self.next_slot) + self.min_interval


# --- HTTP -------------------------------------------------------------------
def http_get(url: str, timeout: int = 10) -> bytes | None:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            # Cap read to 200KB to avoid slow transfers
            return r.read(200_000)
    except Exception:
        return None


# --- CDX query --------------------------------------------------------------
def query_cdx(domain: str, limiter: RateLimiter, max_snapshots: int = 3) -> list[tuple[str, str]]:
    """Return list of (timestamp, original_url) for archived contact/about/team pages."""
    params = {
        "url": f"{domain}/*",
        "output": "json",
        "filter": "urlkey:.*(contact|about|team|staff).*",
        "collapse": "urlkey",
        "fl": "timestamp,original,statuscode",
        "limit": "20",
    }
    url = f"{CDX_API}?{urllib.parse.urlencode(params)}"
    limiter.wait()
    raw = http_get(url, timeout=10)
    if not raw:
        return []
    try:
        data = json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        return []
    if not data or len(data) < 2:
        return []
    # First row is header
    rows = data[1:]
    out: list[tuple[str, str]] = []
    for row in rows:
        if len(row) < 3:
            continue
        ts, orig, sc = row[0], row[1], row[2]
        if sc not in ("200", "-"):
            continue
        out.append((ts, orig))
        if len(out) >= max_snapshots:
            break
    return out


# --- Snapshot fetch & extract ----------------------------------------------
def fetch_snapshot(timestamp: str, original: str, limiter: RateLimiter) -> str | None:
    url = f"{WAYBACK_BASE}/{timestamp}id_/{original}"  # id_ = raw, no wayback toolbar
    limiter.wait()
    raw = http_get(url, timeout=10)
    if not raw:
        return None
    try:
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return None


def valid_email(e: str) -> bool:
    el = e.lower()
    if any(el.endswith(s) for s in BAD_EMAIL_SUFFIXES):
        return False
    if any(el.startswith(p) for p in BAD_EMAIL_PREFIXES):
        return False
    if len(e) > 120:
        return False
    return True


def extract_contacts(html: str) -> tuple[set[str], set[str]]:
    emails = {e for e in EMAIL_RE.findall(html) if valid_email(e)}
    phones = set()
    for m in PHONE_RE.finditer(html):
        area, pre, num = m.group(1), m.group(2), m.group(3)
        phones.add(f"{area}-{pre}-{num}")
    return emails, phones


# --- DB ---------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS wayback_contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    business_id INTEGER,
    domain TEXT,
    email TEXT,
    phone TEXT,
    snapshot_url TEXT,
    snapshot_date TEXT,
    extracted_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_wb_domain ON wayback_contacts(domain);
CREATE INDEX IF NOT EXISTS idx_wb_business ON wayback_contacts(business_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_wb_dedupe ON wayback_contacts(domain, COALESCE(email,''), COALESCE(phone,''));
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


def load_target_domains(conn: sqlite3.Connection, limit: int) -> list[tuple[int, str]]:
    """Prioritize .ca domains with no email AND no phone (dead-site candidates)."""
    cur = conn.execute(
        """
        SELECT id, domain
        FROM businesses
        WHERE domain IS NOT NULL AND length(domain) > 0
          AND (email IS NULL OR email = '')
          AND (phone IS NULL OR phone = '' OR phone LIKE '-%')
          AND domain LIKE '%.ca'
          AND domain NOT IN (SELECT DISTINCT domain FROM wayback_contacts)
        ORDER BY id
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


# --- Worker -----------------------------------------------------------------
def process_domain(
    business_id: int,
    domain: str,
    limiter: RateLimiter,
    max_snapshots: int,
) -> tuple[int, str, list[dict], int]:
    """Returns (business_id, domain, contact_rows, snapshots_fetched)."""
    rows: list[dict] = []
    snaps = query_cdx(domain, limiter, max_snapshots=max_snapshots)
    fetched = 0
    for ts, orig in snaps:
        html = fetch_snapshot(ts, orig, limiter)
        if not html:
            continue
        fetched += 1
        emails, phones = extract_contacts(html)
        snap_url = f"{WAYBACK_BASE}/{ts}/{orig}"
        snap_date = f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"
        for e in emails:
            rows.append({
                "business_id": business_id, "domain": domain,
                "email": e.lower(), "phone": None,
                "snapshot_url": snap_url, "snapshot_date": snap_date,
            })
        for p in phones:
            rows.append({
                "business_id": business_id, "domain": domain,
                "email": None, "phone": p,
                "snapshot_url": snap_url, "snapshot_date": snap_date,
            })
    return business_id, domain, rows, fetched


# --- Main -------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--limit", type=int, default=100, help="Max domains to process")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--rate", type=float, default=3.0, help="Global req/sec cap")
    ap.add_argument("--max-snapshots", type=int, default=2, help="Snapshots per domain")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    ensure_schema(conn)

    targets = load_target_domains(conn, args.limit)
    print(f"[+] Loaded {len(targets)} target domains", flush=True)
    if not targets:
        return 0

    limiter = RateLimiter(args.rate)

    stats = {
        "domains_checked": 0,
        "domains_with_snapshots": 0,
        "snapshots_fetched": 0,
        "emails_extracted": 0,
        "phones_extracted": 0,
        "rows_inserted": 0,
    }
    start = time.monotonic()

    insert_sql = (
        "INSERT OR IGNORE INTO wayback_contacts "
        "(business_id, domain, email, phone, snapshot_url, snapshot_date) "
        "VALUES (:business_id, :domain, :email, :phone, :snapshot_url, :snapshot_date)"
    )

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {
            ex.submit(process_domain, bid, dom, limiter, args.max_snapshots): (bid, dom)
            for bid, dom in targets
        }
        for fut in as_completed(futs):
            bid, dom = futs[fut]
            try:
                _bid, _dom, rows, fetched = fut.result()
            except Exception as e:
                print(f"[!] {dom}: {e}", flush=True)
                continue
            stats["domains_checked"] += 1
            stats["snapshots_fetched"] += fetched
            if fetched:
                stats["domains_with_snapshots"] += 1
            for r in rows:
                if r["email"]:
                    stats["emails_extracted"] += 1
                if r["phone"]:
                    stats["phones_extracted"] += 1
            if rows:
                for attempt in range(5):
                    try:
                        cur = conn.executemany(insert_sql, rows)
                        stats["rows_inserted"] += cur.rowcount if cur.rowcount >= 0 else 0
                        conn.commit()
                        break
                    except sqlite3.OperationalError as e:
                        if "locked" in str(e).lower() and attempt < 4:
                            time.sleep(0.5 * (attempt + 1))
                            continue
                        print(f"[!] DB error for {dom}: {e}", flush=True)
                        break
                    except Exception as e:
                        print(f"[!] DB error for {dom}: {e}", flush=True)
                        break
            if stats["domains_checked"] % 10 == 0:
                elapsed = time.monotonic() - start
                rate = stats["domains_checked"] / max(elapsed, 0.001)
                print(
                    f"[{stats['domains_checked']}/{len(targets)}] "
                    f"snaps={stats['snapshots_fetched']} "
                    f"emails={stats['emails_extracted']} "
                    f"phones={stats['phones_extracted']} "
                    f"({rate:.2f} dom/s)",
                    flush=True,
                )

    elapsed = time.monotonic() - start
    print("\n=== Wayback Harvest Report ===", flush=True)
    print(f"Elapsed:              {elapsed:.1f}s", flush=True)
    print(f"Domains checked:      {stats['domains_checked']}", flush=True)
    print(f"Domains w/ snapshots: {stats['domains_with_snapshots']}", flush=True)
    print(f"Snapshots fetched:    {stats['snapshots_fetched']}", flush=True)
    print(f"Emails extracted:     {stats['emails_extracted']}", flush=True)
    print(f"Phones extracted:     {stats['phones_extracted']}", flush=True)
    print(f"Rows inserted (new):  {stats['rows_inserted']}", flush=True)
    print(f"Finished at:          {datetime.now(timezone.utc).isoformat()}Z", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
