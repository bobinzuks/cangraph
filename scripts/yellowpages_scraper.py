#!/usr/bin/env python3
"""
YellowPages.ca Scraper — trade x city listings with SONA-adaptive rate learning.

Scrapes public business listings (name, phone, address, website, category)
from https://www.yellowpages.ca/search/si/{page}/{trade}/{City+Province}
and stores them in the `yp_listings` table of canada_b2b.db.

Then cross-references listings with the existing `businesses` table via
phone + domain, filling in NULL phones on name matches.

Respectful scraping:
  - Public business data only (no residential listings)
  - 3-5 sec randomized delay between requests (SONA-adapted)
  - User-agent rotation
  - Auto back-off on 429/403
"""
import argparse
import json
import os
import random
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

import requests
from bs4 import BeautifulSoup

# ---------- Config ----------
BASE_URL = "https://www.yellowpages.ca"
SEARCH_TMPL = BASE_URL + "/search/si/{page}/{trade}/{where}"

DEFAULT_DB = os.environ.get("YP_DB", "/opt/cangraph/canada_b2b.db")
STATE_FILE = Path(os.environ.get("YP_SONA_STATE", "/opt/cangraph/data/yp_sona.json"))

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
]

DEFAULT_TRADES = [
    "plumber", "electrician", "roofer", "hvac", "landscaping",
    "painter", "flooring", "drywall", "carpenter", "locksmith",
    "pest-control", "cleaning-services", "handyman", "contractor",
    "renovation", "windows-doors", "fence", "concrete", "paving",
    "moving-services", "auto-repair", "towing", "dentist",
    "accountant", "lawyer", "veterinarian", "chiropractor",
    "massage-therapy", "hair-salon", "barber", "tattoo", "photographer",
    "caterer", "florist", "bakery", "restaurant", "dry-cleaner",
    "pharmacy", "optometrist", "physiotherapist", "daycare",
    "insurance-broker", "real-estate", "mortgage-broker",
    "travel-agency", "tailor", "jeweller", "printing-services",
    "computer-repair", "appliance-repair",
]

DEFAULT_CITIES = [
    ("Toronto", "ON"), ("Montreal", "QC"), ("Vancouver", "BC"),
    ("Calgary", "AB"), ("Edmonton", "AB"), ("Ottawa", "ON"),
    ("Winnipeg", "MB"), ("Quebec-City", "QC"), ("Hamilton", "ON"),
    ("Kitchener", "ON"), ("London", "ON"), ("Halifax", "NS"),
    ("Victoria", "BC"), ("Windsor", "ON"), ("Oshawa", "ON"),
    ("Saskatoon", "SK"), ("Regina", "SK"), ("Saint-John", "NB"),
    ("St-Johns", "NL"), ("Barrie", "ON"),
]

# ---------- SONA rate learning ----------
DEFAULT_STATE = {"delay": 3.5, "successes": 0, "failures": 0, "backoff": 1.0}


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return dict(DEFAULT_STATE)


def save_state(st):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(st, indent=2))


def sona_learn(st, success, status=None):
    if success:
        st["successes"] += 1
        st["delay"] = max(1.5, st["delay"] * 0.97)
        st["backoff"] = max(1.0, st["backoff"] * 0.95)
    else:
        st["failures"] += 1
        if status == 429:
            st["delay"] = min(60.0, st["delay"] * 2.0)
            st["backoff"] = min(10.0, st["backoff"] * 2.0)
        elif status in (403, 503):
            st["delay"] = min(30.0, st["delay"] * 1.8)
            st["backoff"] = min(6.0, st["backoff"] * 1.5)
        else:
            st["delay"] = min(20.0, st["delay"] * 1.3)
            st["backoff"] = min(4.0, st["backoff"] * 1.2)


def sona_sleep(st):
    base = st["delay"] * st["backoff"]
    jitter = random.uniform(0.8, 1.4)
    time.sleep(base * jitter)


# ---------- DB ----------
SCHEMA = """
CREATE TABLE IF NOT EXISTS yp_listings (
    id INTEGER PRIMARY KEY,
    name TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    province TEXT,
    postal_code TEXT,
    website TEXT,
    domain TEXT,
    category TEXT,
    yp_url TEXT UNIQUE,
    scraped_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_yp_phone ON yp_listings(phone);
CREATE INDEX IF NOT EXISTS idx_yp_domain ON yp_listings(domain);
CREATE INDEX IF NOT EXISTS idx_yp_name ON yp_listings(name);
"""


def db_connect(path):
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def upsert_listing(conn, rec):
    conn.execute(
        """INSERT INTO yp_listings
               (name, phone, address, city, province, postal_code,
                website, domain, category, yp_url)
           VALUES (?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(yp_url) DO UPDATE SET
               phone=excluded.phone,
               address=excluded.address,
               website=excluded.website,
               domain=excluded.domain,
               category=excluded.category,
               scraped_at=datetime('now')""",
        (rec["name"], rec["phone"], rec["address"], rec["city"],
         rec["province"], rec["postal_code"], rec["website"],
         rec["domain"], rec["category"], rec["yp_url"]),
    )


# ---------- Parsing ----------
PHONE_RE = re.compile(r"\D+")


def normalize_phone(p):
    if not p:
        return None
    digits = PHONE_RE.sub("", p)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def extract_domain(url):
    if not url:
        return None
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None
    except Exception:
        return None


def resolve_website(a_tag):
    """YP wraps outbound urls as /gourl/<hash>?redirect=<real>."""
    if not a_tag:
        return None
    href = a_tag.get("href", "")
    if "redirect=" in href:
        m = re.search(r"redirect=([^&]+)", href)
        if m:
            return unquote(m.group(1))
    if href.startswith("http"):
        return href
    return None


def clean_name(raw):
    if not raw:
        return None
    # YP prepends a pin-number like "1Drain King Plumbers"
    return re.sub(r"^\d+\s*", "", raw).strip()


def parse_listings(html, search_trade):
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(".listing__content[itemscope]")
    results = []
    for el in items:
        name = clean_name(
            (el.select_one("[itemprop=name]") or {}).get_text(strip=True)
            if el.select_one("[itemprop=name]") else None
        )
        if not name:
            continue

        phone_tag = el.select_one("a[data-phone]")
        phone = normalize_phone(phone_tag.get("data-phone")) if phone_tag else None

        street = el.select_one("[itemprop=streetAddress]")
        locality = el.select_one("[itemprop=addressLocality]")
        region = el.select_one("[itemprop=addressRegion]")
        postal = el.select_one("[itemprop=postalCode]")

        cat_el = el.select_one(".listing__headings")
        category = cat_el.get_text(" ", strip=True) if cat_el else search_trade

        website = resolve_website(
            el.select_one('a[title*="Website" i], a.mlr__item__cta--website, a[href*="/gourl/"]')
        )
        domain = extract_domain(website)

        url_tag = el.select_one('a[href*="/bus/"]')
        yp_url = None
        if url_tag:
            href = url_tag.get("href", "").split("#")[0]
            if href.startswith("/"):
                yp_url = BASE_URL + href
            else:
                yp_url = href

        if not yp_url:
            continue

        results.append({
            "name": name,
            "phone": phone,
            "address": street.get_text(strip=True) if street else None,
            "city": locality.get_text(strip=True) if locality else None,
            "province": region.get_text(strip=True) if region else None,
            "postal_code": postal.get_text(strip=True) if postal else None,
            "website": website,
            "domain": domain,
            "category": category,
            "yp_url": yp_url,
        })
    return results


# ---------- HTTP ----------
def fetch(url, st, session):
    headers = {
        "User-Agent": random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-CA,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": BASE_URL + "/",
        "Connection": "keep-alive",
    }
    try:
        r = session.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            sona_learn(st, True)
            return r.text
        sona_learn(st, False, r.status_code)
        print(f"  HTTP {r.status_code} on {url}", file=sys.stderr)
        return None
    except Exception as e:
        sona_learn(st, False)
        print(f"  ERR {e}", file=sys.stderr)
        return None


def search_trade_city(trade, city, province, pages, st, session):
    where = f"{city}+{province}".replace(" ", "-")
    all_records = []
    for page in range(1, pages + 1):
        url = SEARCH_TMPL.format(page=page, trade=quote(trade), where=where)
        sona_sleep(st)
        html = fetch(url, st, session)
        if not html:
            if st["failures"] > 5 and st["backoff"] > 4.0:
                print("  Excessive failures, aborting", file=sys.stderr)
                return all_records, True  # stop flag
            break
        recs = parse_listings(html, trade)
        if not recs:
            break
        all_records.extend(recs)
        if len(recs) < 20:  # last page heuristic
            break
    return all_records, False


# ---------- Cross-reference ----------
def cross_reference(conn):
    cur = conn.cursor()
    # Matches by phone
    cur.execute("""
        SELECT COUNT(*) FROM yp_listings yp
        JOIN businesses b ON b.phone = yp.phone
        WHERE yp.phone IS NOT NULL AND b.phone IS NOT NULL
    """)
    by_phone = cur.fetchone()[0]

    # Matches by domain
    cur.execute("""
        SELECT COUNT(*) FROM yp_listings yp
        JOIN businesses b ON b.domain = yp.domain
        WHERE yp.domain IS NOT NULL AND b.domain IS NOT NULL
    """)
    by_domain = cur.fetchone()[0]

    # Fill missing phones: matching name + city, businesses.phone IS NULL
    cur.execute("""
        UPDATE businesses
           SET phone = (
               SELECT yp.phone FROM yp_listings yp
                WHERE yp.phone IS NOT NULL
                  AND LOWER(yp.name) = LOWER(businesses.name)
                  AND LOWER(COALESCE(yp.city,'')) = LOWER(COALESCE(businesses.city,''))
                LIMIT 1
           ),
           updated_at = datetime('now')
         WHERE businesses.phone IS NULL
           AND EXISTS (
               SELECT 1 FROM yp_listings yp
                WHERE yp.phone IS NOT NULL
                  AND LOWER(yp.name) = LOWER(businesses.name)
                  AND LOWER(COALESCE(yp.city,'')) = LOWER(COALESCE(businesses.city,''))
           )
    """)
    phones_filled = cur.rowcount
    conn.commit()

    return {
        "matches_by_phone": by_phone,
        "matches_by_domain": by_domain,
        "phones_filled_in_businesses": phones_filled,
    }


# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--trades", nargs="*", default=None,
                    help="Trades to scrape (default: 50-trade list)")
    ap.add_argument("--cities", nargs="*", default=None,
                    help="City:Province pairs (default: 20-city list)")
    ap.add_argument("--pages", type=int, default=1,
                    help="Pages per search (default: 1 = ~35-40 results)")
    ap.add_argument("--limit-trades", type=int, default=None)
    ap.add_argument("--limit-cities", type=int, default=None)
    ap.add_argument("--test", action="store_true",
                    help="Test mode: 5 trades x 2 cities")
    ap.add_argument("--xref-only", action="store_true")
    args = ap.parse_args()

    conn = db_connect(args.db)

    if args.xref_only:
        print("Cross-reference summary:", json.dumps(cross_reference(conn), indent=2))
        return

    trades = args.trades or DEFAULT_TRADES
    cities = []
    if args.cities:
        for c in args.cities:
            if ":" in c:
                a, b = c.split(":", 1)
                cities.append((a, b))
    else:
        cities = DEFAULT_CITIES

    if args.test:
        trades = trades[:5]
        cities = cities[:2]
    else:
        if args.limit_trades:
            trades = trades[: args.limit_trades]
        if args.limit_cities:
            cities = cities[: args.limit_cities]

    st = load_state()
    session = requests.Session()

    total_searches = len(trades) * len(cities)
    total_listings = 0
    searches_done = 0

    print(f"YellowPages scraper: {len(trades)} trades x {len(cities)} cities "
          f"= {total_searches} searches, {args.pages} page(s) each")
    print(f"Current delay={st['delay']:.2f}s backoff={st['backoff']:.2f}x")

    try:
        for trade in trades:
            for city, province in cities:
                searches_done += 1
                print(f"[{searches_done}/{total_searches}] {trade} in {city}, {province}")
                recs, stop = search_trade_city(trade, city, province, args.pages, st, session)
                for rec in recs:
                    upsert_listing(conn, rec)
                conn.commit()
                total_listings += len(recs)
                print(f"  +{len(recs)} listings (total={total_listings}) "
                      f"delay={st['delay']:.2f}s")
                save_state(st)
                if stop:
                    print("Stopping: persistent errors")
                    break
            else:
                continue
            break
    finally:
        save_state(st)
        xref = cross_reference(conn)
        print("\n=== Summary ===")
        print(f"Listings scraped total in DB: "
              f"{conn.execute('SELECT COUNT(*) FROM yp_listings').fetchone()[0]}")
        print(f"This run: +{total_listings} listings from {searches_done} searches")
        print(f"Cross-reference: {json.dumps(xref, indent=2)}")
        conn.close()


if __name__ == "__main__":
    main()
