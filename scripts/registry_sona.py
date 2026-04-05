#!/usr/bin/env python3
"""
Canadian Registry Scraper — SONA-Adaptive Rate Learning

Each runner learns its own safe rate limits per registry.
Distributed across GitHub Actions runners (unique IPs per runner).

SONA principle: Self-Organizing Neural Adaptation
- Start conservative, speed up on success, back off on 429s
- Persist learned rates to state file (commit back to repo)
- Rotate user agents + delay patterns per keyword
"""
import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

# --- SONA State (learned rate limits per registry) ---
STATE_FILE = Path(os.environ.get("SONA_STATE", "data/sona_rates.json"))
DEFAULT_STATE = {
    "gst":      {"delay": 3.0, "successes": 0, "failures": 0, "backoff": 1.0},
    "bc":       {"delay": 1.5, "successes": 0, "failures": 0, "backoff": 1.0},
    "federal":  {"delay": 2.0, "successes": 0, "failures": 0, "backoff": 1.0},
    "ontario":  {"delay": 2.5, "successes": 0, "failures": 0, "backoff": 1.0},
}

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return json.loads(json.dumps(DEFAULT_STATE))  # deep copy


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def sona_learn(state, registry, success, status_code=None):
    """Adjust delay based on outcome.

    Success → reduce delay 5% (learn faster rate)
    429 → double backoff + increase delay 50%
    500/timeout → increase delay 20%
    """
    s = state[registry]
    if success:
        s["successes"] += 1
        # Shrink delay by 5% after each success, min 0.5s
        s["delay"] = max(0.5, s["delay"] * 0.95)
        s["backoff"] = max(1.0, s["backoff"] * 0.9)
    else:
        s["failures"] += 1
        if status_code == 429:
            s["backoff"] = min(60.0, s["backoff"] * 2.0)
            s["delay"] = min(30.0, s["delay"] * 1.5)
        else:
            s["delay"] = min(15.0, s["delay"] * 1.2)
    return s


def sona_sleep(state, registry):
    """Sleep with learned delay + jitter + active backoff."""
    s = state[registry]
    base = s["delay"] * s["backoff"]
    jitter = random.uniform(0, base * 0.3)
    time.sleep(base + jitter)


# --- Registry Endpoints ---
REGISTRIES = {
    "bc": {
        "url": "https://www.bcregistry.gov.bc.ca/search",
        "params": lambda kw: {"q": kw},
    },
    "federal": {
        "url": "https://www.ic.gc.ca/app/scr/cc/CorporationsCanada/fdrlCrpSrch.html",
        "params": lambda kw: {"srchNm": kw},
    },
    "ontario": {
        "url": "https://www.ontario.ca/page/search-business-registry",
        "params": lambda kw: {"search": kw},
    },
}


def scrape_one(registry, keyword, state, session):
    """Single scrape with SONA-adaptive delay."""
    cfg = REGISTRIES.get(registry)
    if not cfg:
        return {"registry": registry, "keyword": keyword, "error": "unknown registry"}

    session.headers["User-Agent"] = random.choice(UA_POOL)
    sona_sleep(state, registry)

    try:
        r = session.get(cfg["url"], params=cfg["params"](keyword), timeout=20)
        status = r.status_code

        if status == 200:
            sona_learn(state, registry, True)
            soup = BeautifulSoup(r.text, "html.parser")
            results = []
            for el in soup.find_all(["h3", "h4", "a", "strong"]):
                text = el.get_text(strip=True)
                if 5 < len(text) < 150:
                    results.append(text)
            return {
                "registry": registry,
                "keyword": keyword,
                "status": status,
                "results": results[:50],
                "delay_used": state[registry]["delay"],
            }

        sona_learn(state, registry, False, status)
        return {"registry": registry, "keyword": keyword, "status": status, "error": f"HTTP {status}"}

    except Exception as e:
        sona_learn(state, registry, False)
        return {"registry": registry, "keyword": keyword, "error": str(e)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", required=True, choices=list(REGISTRIES.keys()) + ["all"])
    ap.add_argument("--keywords", required=True, help="Comma-separated keywords")
    ap.add_argument("--output", default="data/registry_results.jsonl")
    ap.add_argument("--max-per-registry", type=int, default=100)
    ap.add_argument("--runner-id", default=os.environ.get("RUNNER_ID", "local"))
    args = ap.parse_args()

    state = load_state()
    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    registries = [args.registry] if args.registry != "all" else list(REGISTRIES.keys())

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    count, success_count = 0, 0
    start = time.time()

    print(f"[SONA] Runner={args.runner_id} | Registries={registries} | Keywords={len(keywords)}")
    print(f"[SONA] Starting delays: {json.dumps({r: state[r]['delay'] for r in registries})}")

    with out_path.open("a") as f:
        for registry in registries:
            per_reg = 0
            for kw in keywords:
                if per_reg >= args.max_per_registry:
                    break
                result = scrape_one(registry, kw, state, session)
                result["runner_id"] = args.runner_id
                result["timestamp"] = datetime.now(timezone.utc).isoformat()
                f.write(json.dumps(result) + "\n")
                count += 1
                per_reg += 1
                if "results" in result:
                    success_count += 1
                    print(f"  [{registry}] {kw}: {len(result['results'])} hits (delay={state[registry]['delay']:.2f}s)")
                else:
                    print(f"  [{registry}] {kw}: {result.get('error', 'failed')} (backoff={state[registry]['backoff']:.1f}x)")

                # Persist state every 10 requests
                if count % 10 == 0:
                    save_state(state)

    save_state(state)
    elapsed = time.time() - start
    print(f"\n[SONA] Done. {success_count}/{count} OK in {elapsed:.0f}s")
    print(f"[SONA] Final learned delays: {json.dumps({r: round(state[r]['delay'], 2) for r in registries})}")


if __name__ == "__main__":
    main()
