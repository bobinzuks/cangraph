#!/usr/bin/env python3
"""
tracking_expander.py - Extend master_web_map.db tracking-ID graph with
additional trackers (FB Pixel, Shopify, Stripe, Mailchimp, HubSpot, Intercom,
LinkedIn Insight, TikTok Pixel, Hotjar, Plausible, Google Ads) to discover
more owner clusters.

The existing DB stores only extracted tracking IDs - no raw HTML. So this
script re-crawls domains (first 40KB of index page), extracts the new
patterns, stores them in the existing `tracking_ids` / `domain_ids` tables
with new `type` values, then re-computes owner clusters via union-find on
any shared tracking ID.

Usage:
    # Test on 100 random fetched domains
    python3 tracking_expander.py --db /opt/cangraph/master_web_map.db --sample 100

    # Full re-crawl (841K domains). WARNING: takes hours, needs confirmation
    python3 tracking_expander.py --db /opt/cangraph/master_web_map.db --all --confirm

    # Only re-cluster from existing data (no crawl)
    python3 tracking_expander.py --db /opt/cangraph/master_web_map.db --recluster
"""
import argparse
import json
import random
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------------------------------------------------------- patterns
# Each pattern tuple: (tracker_type, compiled_regex, group_index)
PATTERNS = [
    # 1. Facebook Pixel
    ("fb_pixel", re.compile(r"fbq\(\s*['\"]init['\"],\s*['\"](\d{10,20})['\"]"), 1),
    ("fb_pixel", re.compile(r"facebook\.com/tr\?id=(\d{10,20})"), 1),
    # 2. Shopify
    ("shopify_id", re.compile(r"Shopify\.shop\s*=\s*['\"]([^'\"]+?\.myshopify\.com)"), 1),
    ("shopify_id", re.compile(r"cdn\.shopify\.com/s/files/1/(\d+)/"), 1),
    # 3. Stripe publishable keys
    ("stripe_pk", re.compile(r"(pk_live_[A-Za-z0-9]{24,})"), 1),
    ("stripe_pk", re.compile(r"(pk_test_[A-Za-z0-9]{24,})"), 1),
    # 4. Mailchimp list id (32 hex)
    ("mailchimp_list", re.compile(r"[a-z0-9]+\.list-manage\.com/[^\"'\s]*?([a-f0-9]{32})"), 1),
    # 5. HubSpot
    ("hubspot_id", re.compile(r"js\.hs-scripts\.com/(\d+)\.js"), 1),
    ("hubspot_id", re.compile(r"js\.hsforms\.net/forms/.*?portalId['\"]?\s*:\s*['\"]?(\d+)"), 1),
    # 6. Intercom app id
    ("intercom_app", re.compile(r"intercomSettings[^{}]*?app_id['\"]?\s*:\s*['\"]([a-z0-9]{4,12})['\"]", re.S), 1),
    ("intercom_app", re.compile(r"widget\.intercom\.io/widget/([a-z0-9]{4,12})"), 1),
    # 7. Cloudflare Turnstile site keys (format 0x4...)
    ("turnstile", re.compile(r"turnstile[^\"']{0,80}?['\"](0x[A-Za-z0-9_\-]{20,})['\"]", re.I), 1),
    ("turnstile", re.compile(r"data-sitekey=['\"](0x[A-Za-z0-9_\-]{20,})['\"]"), 1),
    # 8. Google Ads (already partially done, but grab AW-*)
    ("google_ads_aw", re.compile(r"(AW-\d{8,12})"), 1),
    # 9. LinkedIn Insight
    ("linkedin_insight", re.compile(r"_linkedin_partner_id\s*=\s*['\"]?(\d+)"), 1),
    # 10. TikTok Pixel
    ("tiktok_pixel", re.compile(r"ttq\.load\(\s*['\"]([A-Z0-9]{10,30})['\"]"), 1),
    # 11. Hotjar
    ("hotjar_sid", re.compile(r"hjid\s*[:=]\s*(\d+)"), 1),
    ("hotjar_sid", re.compile(r"static\.hotjar\.com/c/hotjar-(\d+)\.js"), 1),
    # 12. Plausible
    ("plausible_domain", re.compile(r"plausible\.io/js/[^\"']+\s*['\"]?\s*data-domain=['\"]([^'\"]+)['\"]"), 1),
    ("plausible_domain", re.compile(r"data-domain=['\"]([^'\"]+)['\"][^>]*src=['\"][^'\"]*plausible"), 1),
]

TYPES = sorted(set(t for t, _, _ in PATTERNS))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CangraphTracker/1.0; +https://cangraph.ca/)"
}


# ------------------------------------------------------------- http helpers
def make_session():
    s = requests.Session()
    retry = Retry(total=1, backoff_factor=0.3, status_forcelist=[502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50))
    return s


def fetch_html(session, domain, max_bytes=40960, timeout=8):
    for scheme in ("https://", "http://"):
        try:
            r = session.get(scheme + domain, headers=HEADERS, timeout=timeout,
                            stream=True, allow_redirects=True, verify=False)
            if r.status_code >= 400:
                r.close()
                continue
            raw = r.raw.read(max_bytes, decode_content=True)
            r.close()
            return raw.decode("utf-8", errors="ignore")
        except Exception:
            continue
    return None


def extract_new_ids(html):
    """Returns dict of {id: type}."""
    found = {}
    for ttype, pat, grp in PATTERNS:
        for m in pat.finditer(html):
            try:
                val = m.group(grp)
            except IndexError:
                continue
            if val and len(val) < 200:
                # don't overwrite if already found under different type
                found.setdefault(val, ttype)
    return found


# --------------------------------------------------------------------- db ops
def ensure_schema(conn):
    # Existing schema already works; we just add a column to track
    # when a domain was scanned for new trackers so we can resume.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(domains)")}
    if "tracker_expanded_at" not in cols:
        conn.execute("ALTER TABLE domains ADD COLUMN tracker_expanded_at TEXT")
        conn.commit()


def store_ids(conn, domain, id_map):
    for tid, ttype in id_map.items():
        conn.execute(
            "INSERT OR IGNORE INTO tracking_ids(id, type) VALUES(?, ?)", (tid, ttype)
        )
        conn.execute(
            "INSERT OR IGNORE INTO domain_ids(domain, tracking_id) VALUES(?, ?)",
            (domain, tid),
        )
    conn.execute(
        "UPDATE domains SET tracker_expanded_at = datetime('now') WHERE domain = ?",
        (domain,),
    )


# -------------------------------------------------------------------- crawl
def crawl_worker(domain, session):
    html = fetch_html(session, domain)
    if not html:
        return domain, {}
    return domain, extract_new_ids(html)


def crawl_domains(conn, domains, workers=32):
    session = make_session()
    stats = {"scanned": 0, "with_hits": 0, "new_ids": 0, "by_type": {}}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(crawl_worker, d, session): d for d in domains}
        for i, fut in enumerate(as_completed(futs)):
            domain, id_map = fut.result()
            stats["scanned"] += 1
            if id_map:
                stats["with_hits"] += 1
                stats["new_ids"] += len(id_map)
                for _, t in id_map.items():
                    stats["by_type"][t] = stats["by_type"].get(t, 0) + 1
                store_ids(conn, domain, id_map)
            else:
                conn.execute(
                    "UPDATE domains SET tracker_expanded_at = datetime('now') WHERE domain = ?",
                    (domain,),
                )
            if (i + 1) % 50 == 0:
                conn.commit()
                rate = stats["scanned"] / (time.time() - t0 + 0.1)
                print(f"  [{i+1}/{len(domains)}] hits={stats['with_hits']} "
                      f"new_ids={stats['new_ids']} {rate:.1f}/s", flush=True)
    conn.commit()
    stats["elapsed_s"] = round(time.time() - t0, 1)
    return stats


# -------------------------------------------------------------- clustering
def recluster(conn):
    """Union-find over all (domain <-> tracking_id) edges.
    Two domains share a cluster if they share ANY tracking ID."""
    print("[recluster] loading edges...", flush=True)
    parent = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # Skip super-generic tracking IDs (>500 domains) - they're shared services
    # not ownership signals.
    generic = {
        r[0] for r in conn.execute(
            "SELECT id FROM tracking_ids WHERE domain_count > 500"
        )
    }
    print(f"[recluster] ignoring {len(generic)} super-generic IDs (>500 domains)",
          flush=True)

    # Group domains by tracking ID
    cur = conn.execute(
        "SELECT tracking_id, domain FROM domain_ids ORDER BY tracking_id"
    )
    current_tid = None
    bucket = []
    edges = 0
    for tid, dom in cur:
        if tid in generic:
            continue
        parent.setdefault(dom, dom)
        if tid != current_tid:
            if len(bucket) > 1:
                anchor = bucket[0]
                for d in bucket[1:]:
                    union(anchor, d)
                    edges += 1
            bucket = [dom]
            current_tid = tid
        else:
            bucket.append(dom)
    if len(bucket) > 1:
        anchor = bucket[0]
        for d in bucket[1:]:
            union(anchor, d)
            edges += 1
    print(f"[recluster] union-find done: {edges} edges, {len(parent)} nodes",
          flush=True)

    # Group by root
    clusters = {}
    for d in parent:
        r = find(d)
        clusters.setdefault(r, []).append(d)
    clusters = {k: v for k, v in clusters.items() if len(v) > 1}

    sizes = sorted((len(v) for v in clusters.values()), reverse=True)
    print(f"[recluster] {len(clusters)} clusters, top sizes: {sizes[:10]}",
          flush=True)

    # Rewrite clusters table
    conn.execute("DELETE FROM clusters")
    for root, members in clusters.items():
        # pick the tracking ID with the smallest domain_count that ties these
        anchor_tid = conn.execute(
            """SELECT tracking_id FROM domain_ids
               WHERE domain IN (%s)
               AND tracking_id NOT IN (SELECT id FROM tracking_ids WHERE domain_count>500)
               GROUP BY tracking_id
               ORDER BY COUNT(*) DESC LIMIT 1"""
            % ",".join("?" * min(len(members), 50)),
            members[:50],
        ).fetchone()
        conn.execute(
            "INSERT INTO clusters(anchor_id, domains, owner_signal, size) VALUES(?,?,?,?)",
            (
                anchor_tid[0] if anchor_tid else root,
                json.dumps(members[:500]),
                "tracking_id",
                len(members),
            ),
        )
    conn.commit()
    return {"clusters": len(clusters), "top_sizes": sizes[:10]}


# --------------------------------------------------------------------- main
def pick_domains(conn, sample_size, only_unexpanded=True):
    if only_unexpanded:
        rows = conn.execute(
            "SELECT domain FROM domains "
            "WHERE fetched=1 AND (tracker_expanded_at IS NULL) "
            "ORDER BY RANDOM() LIMIT ?",
            (sample_size,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT domain FROM domains WHERE fetched=1 ORDER BY RANDOM() LIMIT ?",
            (sample_size,),
        ).fetchall()
    return [r[0] for r in rows]


def summary(conn, before_counts):
    after_counts = dict(conn.execute(
        "SELECT type, COUNT(*) FROM tracking_ids GROUP BY type"
    ).fetchall())
    delta = {}
    for t in set(list(before_counts) + list(after_counts)):
        d = after_counts.get(t, 0) - before_counts.get(t, 0)
        if d:
            delta[t] = d
    return {"before": before_counts, "after": after_counts, "delta": delta}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--sample", type=int, default=0, help="crawl N random unexpanded domains")
    ap.add_argument("--all", action="store_true", help="crawl ALL fetched domains")
    ap.add_argument("--confirm", action="store_true", help="confirm --all re-crawl")
    ap.add_argument("--recluster", action="store_true", help="re-run clustering only")
    ap.add_argument("--workers", type=int, default=32)
    args = ap.parse_args()

    # silence insecure request warnings for verify=False
    try:
        from urllib3 import disable_warnings
        from urllib3.exceptions import InsecureRequestWarning
        disable_warnings(InsecureRequestWarning)
    except Exception:
        pass

    conn = sqlite3.connect(args.db, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    ensure_schema(conn)

    before = dict(conn.execute(
        "SELECT type, COUNT(*) FROM tracking_ids GROUP BY type"
    ).fetchall())
    print(f"[start] tracking_ids by type: {before}", flush=True)

    if args.recluster and not (args.sample or args.all):
        r = recluster(conn)
        print(json.dumps(r, indent=2))
        return

    if args.all and not args.confirm:
        total = conn.execute(
            "SELECT COUNT(*) FROM domains WHERE fetched=1 AND tracker_expanded_at IS NULL"
        ).fetchone()[0]
        print(f"[abort] --all would crawl {total} domains. Re-run with --confirm.")
        sys.exit(2)

    if args.all:
        sample_size = conn.execute(
            "SELECT COUNT(*) FROM domains WHERE fetched=1 AND tracker_expanded_at IS NULL"
        ).fetchone()[0]
    else:
        sample_size = args.sample or 100

    domains = pick_domains(conn, sample_size)
    print(f"[crawl] picked {len(domains)} domains (workers={args.workers})", flush=True)
    stats = crawl_domains(conn, domains, workers=args.workers)
    print(f"[crawl] {json.dumps(stats, indent=2)}", flush=True)

    s = summary(conn, before)
    print(f"[delta] new tracker IDs added by type: {json.dumps(s['delta'], indent=2)}",
          flush=True)

    print("[recluster] re-running owner clustering...", flush=True)
    r = recluster(conn)
    print(json.dumps(r, indent=2))

    largest = conn.execute(
        "SELECT anchor_id, size FROM clusters ORDER BY size DESC LIMIT 10"
    ).fetchall()
    print(f"[result] top 10 clusters: {largest}", flush=True)


if __name__ == "__main__":
    main()
