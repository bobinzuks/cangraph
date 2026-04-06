#!/usr/bin/env python3
"""Import tracker results from JSONL into master_web_map.db"""
import json, sqlite3, sys

DB = sys.argv[1] if len(sys.argv) > 1 else "/opt/cangraph/master_web_map.db"
JSONL = sys.argv[2] if len(sys.argv) > 2 else "/opt/cangraph/data/all_trackers.jsonl"

conn = sqlite3.connect(DB)
conn.execute("PRAGMA journal_mode=WAL")
added = 0
with open(JSONL) as f:
    for line in f:
        r = json.loads(line)
        domain = r["domain"]
        for ttype, ids in r.get("ids", {}).items():
            for tid in ids:
                full_id = f"{ttype}:{tid}"
                try:
                    conn.execute("INSERT OR IGNORE INTO tracking_ids (type, id) VALUES (?, ?)", (ttype, full_id))
                    row = conn.execute("SELECT id FROM tracking_ids WHERE type=? AND id=?", (ttype, full_id)).fetchone()
                    if row:
                        conn.execute("INSERT OR IGNORE INTO domain_ids (domain, tracking_id) VALUES (?, ?)", (domain, row[0]))
                        added += 1
                except:
                    pass
        conn.execute("UPDATE domains SET tracker_expanded_at = datetime('now') WHERE domain = ?", (domain,))
conn.commit()
print(f"Added {added} domain-tracker links")
conn.close()
