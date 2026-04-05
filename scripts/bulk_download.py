#!/usr/bin/env python3
"""
Bulk download Canadian business data from OFFICIAL open datasets.
No scraping, no rate limits, no TOS violations.

Sources:
- Corporations Canada (Federal): open.canada.ca bulk JSON
- Ontario: data.ontario.ca open data
- Open Corporates mirrors
"""
import json
import os
import sys
import sqlite3
from pathlib import Path
from urllib.request import urlopen, Request

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

SOURCES = {
    "federal": {
        "url": "https://ised-isde.canada.ca/site/corporations-canada/en/accessing-federal-corporation-json-datasets",
        "description": "Corporations Canada federal JSON dataset (updated monthly)",
        "note": "Manual download required — check landing page for current file",
    },
    "open_canada": {
        "url": "https://open.canada.ca/data/en/dataset/0032ce54-c5dd-4b66-99a0-320a7b5e99f2",
        "description": "Open Government Federal Corporations dataset",
    },
    "ontario_obr": {
        "url": "https://data.ontario.ca/api/3/action/package_show?id=ontario-business-registry-partner-portal",
        "description": "Ontario Business Registry partner portal info",
    },
}


def fetch_json(url):
    req = Request(url, headers={"User-Agent": "CanGraph/1.0 research"})
    with urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def main():
    print("=== Canadian Registry Bulk Sources ===\n")
    for key, src in SOURCES.items():
        print(f"[{key}]")
        print(f"  URL: {src['url']}")
        print(f"  Description: {src['description']}")
        if "note" in src:
            print(f"  Note: {src['note']}")
        print()

    print("\n=== Fetching Ontario OBR metadata ===\n")
    try:
        data = fetch_json(SOURCES["ontario_obr"]["url"])
        result = data.get("result", {})
        print(f"Title: {result.get('title')}")
        print(f"Organization: {result.get('organization', {}).get('title', 'N/A')}")
        resources = result.get("resources", [])
        print(f"Resources: {len(resources)}")
        for r in resources[:10]:
            print(f"  - {r.get('name', 'unnamed')}: {r.get('url', 'no url')}")
            print(f"    Format: {r.get('format', 'unknown')} | Size: {r.get('size', 'unknown')}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
