#!/usr/bin/env python3
"""
NPA-NXX Phone Enrichment
Downloads CNAC (Canadian Numbering Administration Consortium) CO Code Status
CSVs for every Canadian NPA and uses them to enrich phone numbers in
canada_b2b.db with carrier, OCN, rate center, and province.

Data source: http://cnac.ca/data/COCodeStatus_NPA{NPA}.csv
"""

import csv
import io
import os
import re
import sqlite3
import sys
import time
import urllib.request

DB_PATH = os.environ.get("CANGRAPH_DB", "/opt/cangraph/canada_b2b.db")
CNAC_URL = "http://cnac.ca/data/COCodeStatus_NPA{npa}.csv"

# All active Canadian NPAs (as of 2026-04, scraped from CNAC map)
CA_NPAS = [
    "204", "226", "236", "249", "250", "257", "263", "273", "289", "306",
    "343", "354", "365", "367", "368", "382", "403", "416", "418", "428",
    "431", "437", "438", "450", "468", "474", "506", "514", "519", "548",
    "579", "581", "584", "587", "604", "613", "639", "647", "672", "683",
    "705", "709", "742", "753", "778", "780", "782", "807", "819", "825",
    "867", "873", "879", "902", "905", "942",
]


def fetch_npa_csv(npa: str) -> list[dict]:
    """Download and parse one NPA's CO code status CSV."""
    url = CNAC_URL.format(npa=npa)
    req = urllib.request.Request(url, headers={"User-Agent": "cangraph/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(raw))
    rows = []
    for r in reader:
        # Skip the timestamp row (all fields blank except "Remarks"/last col)
        if not r.get("NPA") or not r.get("CO Code (NXX)"):
            continue
        rows.append({
            "npa": (r.get("NPA") or "").strip(),
            "nxx": (r.get("CO Code (NXX)") or "").strip(),
            "status": (r.get("Status") or "").strip(),
            "rate_center": (r.get("Exchange Area") or "").strip(),
            "province": (r.get("Province") or "").strip(),
            "carrier_name": (r.get("Company") or "").strip(),
            "ocn": (r.get("OCN") or "").strip(),
        })
    return rows


def build_lookup_table(conn: sqlite3.Connection) -> int:
    """Create npa_nxx_lookup table and populate from CNAC."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS npa_nxx_lookup (
            npa TEXT NOT NULL,
            nxx TEXT NOT NULL,
            status TEXT,
            carrier_name TEXT,
            ocn TEXT,
            rate_center TEXT,
            province TEXT,
            PRIMARY KEY (npa, nxx)
        )
    """)
    conn.commit()

    total = 0
    for i, npa in enumerate(CA_NPAS, 1):
        try:
            rows = fetch_npa_csv(npa)
        except Exception as e:
            print(f"[{i}/{len(CA_NPAS)}] NPA {npa}: FAIL {e}", file=sys.stderr)
            continue
        cur.executemany("""
            INSERT OR REPLACE INTO npa_nxx_lookup
                (npa, nxx, status, carrier_name, ocn, rate_center, province)
            VALUES (:npa, :nxx, :status, :carrier_name, :ocn, :rate_center, :province)
        """, rows)
        conn.commit()
        total += len(rows)
        print(f"[{i}/{len(CA_NPAS)}] NPA {npa}: {len(rows)} NXX rows")
        time.sleep(0.2)  # be polite to CNAC
    return total


# Extract 10-digit Canadian phone: strip non-digits, drop leading 1 if 11-digit
PHONE_RE = re.compile(r"\D+")


def normalize_phone(raw: str) -> str | None:
    if not raw:
        return None
    digits = PHONE_RE.sub("", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    # NPA must start with 2-9 and be a known CA NPA
    if digits[0] == "0" or digits[0] == "1":
        return None
    return digits


def enrich_phones(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_enrichment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id INTEGER,
            phone TEXT,
            npa TEXT,
            nxx TEXT,
            carrier_name TEXT,
            ocn TEXT,
            rate_center TEXT,
            province TEXT,
            country TEXT DEFAULT 'CA'
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pe_business ON phone_enrichment(business_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pe_npa_nxx ON phone_enrichment(npa, nxx)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pe_ocn ON phone_enrichment(ocn)")
    # Idempotent: clear previous enrichment rows
    cur.execute("DELETE FROM phone_enrichment")
    conn.commit()

    cur.execute("""
        SELECT id, phone FROM businesses
        WHERE phone IS NOT NULL AND length(phone) > 0
    """)
    scanned = 0
    normalized = 0
    enriched = 0
    batch = []
    BATCH_N = 5000

    # Pre-load the lookup as a dict for speed
    lookup: dict[tuple[str, str], tuple] = {}
    cur2 = conn.cursor()
    cur2.execute("""
        SELECT npa, nxx, carrier_name, ocn, rate_center, province
        FROM npa_nxx_lookup
    """)
    for row in cur2:
        lookup[(row[0], row[1])] = row[2:]
    print(f"Loaded {len(lookup)} NPA-NXX rows into memory")

    for bid, phone in cur:
        scanned += 1
        norm = normalize_phone(phone)
        if not norm:
            continue
        normalized += 1
        npa, nxx = norm[:3], norm[3:6]
        hit = lookup.get((npa, nxx))
        if not hit:
            continue
        carrier_name, ocn, rate_center, province = hit
        batch.append((bid, norm, npa, nxx, carrier_name, ocn, rate_center, province))
        enriched += 1
        if len(batch) >= BATCH_N:
            cur2.executemany("""
                INSERT INTO phone_enrichment
                    (business_id, phone, npa, nxx, carrier_name, ocn, rate_center, province)
                VALUES (?,?,?,?,?,?,?,?)
            """, batch)
            conn.commit()
            batch.clear()

    if batch:
        cur2.executemany("""
            INSERT INTO phone_enrichment
                (business_id, phone, npa, nxx, carrier_name, ocn, rate_center, province)
            VALUES (?,?,?,?,?,?,?,?)
        """, batch)
        conn.commit()

    return {"scanned": scanned, "normalized": normalized, "enriched": enriched}


def top_carriers(conn: sqlite3.Connection, n: int = 5) -> list[tuple[str, int]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT carrier_name, COUNT(*) c FROM phone_enrichment
        GROUP BY carrier_name ORDER BY c DESC LIMIT ?
    """, (n,))
    return cur.fetchall()


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Only rebuild lookup if empty or --refresh
    refresh = "--refresh" in sys.argv
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master WHERE type='table' AND name='npa_nxx_lookup'
    """)
    has_lookup = cur.fetchone() is not None
    count = 0
    if has_lookup:
        cur.execute("SELECT COUNT(*) FROM npa_nxx_lookup")
        count = cur.fetchone()[0]

    if refresh or not has_lookup or count == 0:
        print("Building NPA-NXX lookup from CNAC...")
        total = build_lookup_table(conn)
        print(f"Lookup table built: {total} rows")
    else:
        print(f"Using existing npa_nxx_lookup ({count} rows). Pass --refresh to rebuild.")

    print("Enriching phones from businesses table...")
    stats = enrich_phones(conn)
    print(f"Scanned: {stats['scanned']}  Normalized: {stats['normalized']}  Enriched: {stats['enriched']}")

    print("\nTop 5 carriers:")
    for name, c in top_carriers(conn, 5):
        print(f"  {c:>8}  {name}")

    conn.close()


if __name__ == "__main__":
    main()
