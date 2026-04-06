#!/usr/bin/env python3
"""Import wayback harvest results from JSONL into canada_b2b.db"""
import json, sqlite3, sys

DB = sys.argv[1] if len(sys.argv) > 1 else "/opt/cangraph/canada_b2b.db"
JSONL = sys.argv[2] if len(sys.argv) > 2 else "/opt/cangraph/data/wayback_merged.jsonl"

conn = sqlite3.connect(DB)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("""CREATE TABLE IF NOT EXISTS wayback_contacts (
    id INTEGER PRIMARY KEY, business_id INTEGER, domain TEXT,
    email TEXT, phone TEXT, snapshot_url TEXT, snapshot_date TEXT,
    extracted_at TEXT DEFAULT (datetime('now'))
)""")
count = 0
with open(JSONL) as f:
    for line in f:
        r = json.loads(line)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO wayback_contacts (domain, email, phone, extracted_at) VALUES (?,?,?,?)",
                (r.get("domain"), r.get("email"), r.get("phone"), r.get("ts")))
            count += 1
        except:
            pass
conn.commit()
print(f"Imported {count} wayback contacts")
conn.close()
