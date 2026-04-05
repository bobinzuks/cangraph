"""
Pi Integration for CanGraph -- Sona Embeddings, HNSW Search, Mincut Partitioning,
Byzantine Confidence Scoring, and 6 new MCP tools.
Local-first with numpy; Pi (pi.ruv.io) sync optional.

CLI:
  python3 pi_integration.py --embed|--index|--partition|--score|--all|--stats
"""
import argparse, hashlib, json, os, sqlite3, sys, time
from contextlib import contextmanager
from pathlib import Path
import numpy as np

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.environ.get("CANGRAPH_DB",
    str(BASE_DIR.parent / "email-velocity" / "data" / "canada_b2b.db"))
DATA_DIR = BASE_DIR / "data"; DATA_DIR.mkdir(exist_ok=True)
HNSW_PATH = DATA_DIR / "hnsw_index.bin"
EMBED_DIM, BATCH, K_PARTS = 128, 1000, 20
SOURCE_BITS = {"permits": 0, "gmb": 1, "registries": 2, "yellowpages": 3, "common_crawl": 5}
SOURCE_WEIGHTS = {"permits": 0.95, "gmb": 0.85, "registries": 0.90,
                  "yellowpages": 0.60, "common_crawl": 0.50}

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    try: yield conn
    finally: conn.close()

def _ensure_col():
    with get_db() as c:
        if "partition_id" not in {r[1] for r in c.execute("PRAGMA table_info(businesses)")}:
            c.execute("ALTER TABLE businesses ADD COLUMN partition_id INTEGER DEFAULT NULL")
            c.commit()

def _norm(v):
    n = np.linalg.norm(v)
    return v / n if n > 0 else v

# -- 1. SONA EMBEDDINGS --
def _hash_tok(text, dim):
    v = np.zeros(dim, dtype=np.float32)
    if not text: return v
    for tok in text.lower().split():
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
        for i in range(dim): v[i] += 1.0 if (h >> (i % 128)) & 1 else -1.0
    return _norm(v)

def _enc_cat(val, dim, salt=""):
    v = np.zeros(dim, dtype=np.float32)
    if not val: return v
    for b in hashlib.sha256(f"{salt}:{val}".encode()).digest()[:16]: v[b % dim] = 1.0
    return _norm(v)

def _enc_src(flags, dim):
    v = np.zeros(dim, dtype=np.float32)
    for i in range(8):
        if flags & (1 << i): v += np.random.RandomState(42 + i).randn(dim).astype(np.float32) * 0.3
    return _norm(v)

def compute_embedding(r):
    s = np.zeros(16, dtype=np.float32)
    s[0] = min((r.get("confidence") or 0) / 100.0, 1.0)
    s[1] = 1.0 if r.get("email") and r["email"] not in ("", "None") else 0.0
    s[2] = 1.0 if r.get("phone") and r["phone"] not in ("", "None") else 0.0
    s[3] = 1.0 if r.get("owner_cluster_id") else 0.0
    s[4] = min((r.get("contact_quality_score") or 0) / 100.0, 1.0)
    v = np.concatenate([_hash_tok(r.get("name") or "", 32),
        _enc_cat(r.get("trade") or "", 24, "trade"),
        _enc_cat(r.get("city") or "", 24, "city"),
        _enc_cat(r.get("province") or "", 16, "province"),
        _enc_src(r.get("source_flags") or 0, 16), s])
    return _norm(v)

def _ideal_vec():
    return compute_embedding({"name": "Ideal Premium Business", "trade": "General Contractor",
        "city": "Toronto", "province": "ON", "source_flags": 0b111111, "confidence": 100,
        "email": "a@b.ca", "phone": "4161234567", "owner_cluster_id": "c1",
        "contact_quality_score": 100})

def step_embed():
    print("[EMBED] Generating 128-dim embeddings...")
    ideal, done = _ideal_vec(), 0
    with get_db() as conn:
        tot = conn.execute("SELECT count(*) FROM businesses").fetchone()[0]
        off = 0
        while off < tot:
            rows = conn.execute(
                "SELECT id,name,trade,city,province,source_flags,confidence,"
                "email,phone,owner_cluster_id,contact_quality_score "
                "FROM businesses LIMIT ? OFFSET ?", (BATCH, off)).fetchall()
            if not rows: break
            ups = []
            for r in rows:
                d = dict(r); v = compute_embedding(d)
                score = round(max(0.0, min(1.0, (float(np.dot(v, ideal)) + 1) / 2)), 4)
                ups.append((json.dumps(v.tolist()), score, d["id"]))
            conn.executemany("UPDATE businesses SET gnn_vectors=?,gnn_score=? WHERE id=?", ups)
            conn.commit(); done += len(ups); off += BATCH
            sys.stdout.write(f"\r[EMBED] {done}/{tot}"); sys.stdout.flush()
    print(f"\n[EMBED] Done. {done} embeddings.")

# -- 2. HNSW SEARCH --
class BruteForceIndex:
    def __init__(self, dim): self.dim, self.ids, self.vectors = dim, [], None
    def add_items(self, vecs, ids): self.vectors, self.ids = vecs.astype(np.float32), list(ids)
    def knn_query(self, q, k=10):
        sims = (self.vectors @ q.reshape(-1)).flatten()
        topk = min(k, len(self.ids))
        idx = np.argpartition(-sims, topk)[:topk]
        idx = idx[np.argsort(-sims[idx])]
        return np.array([[self.ids[i] for i in idx]]), np.array([1 - sims[idx]])
    def save_index(self, p): np.savez(p, vectors=self.vectors, ids=np.array(self.ids))
    def load_index(self, p):
        d = np.load(p + ".npz" if not p.endswith(".npz") else p)
        self.vectors, self.ids = d["vectors"], d["ids"].tolist()

def _load_embeddings():
    ids, vecs = [], []
    with get_db() as conn:
        off = 0
        while True:
            rows = conn.execute("SELECT id,gnn_vectors FROM businesses WHERE gnn_vectors IS NOT NULL LIMIT ? OFFSET ?",
                (BATCH, off)).fetchall()
            if not rows: break
            for r in rows: ids.append(r["id"]); vecs.append(json.loads(r["gnn_vectors"]))
            off += BATCH
    return ids, np.array(vecs, dtype=np.float32) if vecs else np.empty((0, EMBED_DIM))

def build_index(ids, vecs):
    try:
        import hnswlib
        idx = hnswlib.Index(space="cosine", dim=EMBED_DIM)
        idx.init_index(max_elements=len(ids), ef_construction=200, M=16)
        idx.add_items(vecs, ids); idx.set_ef(50); idx.save_index(str(HNSW_PATH))
        print(f"[INDEX] hnswlib saved to {HNSW_PATH}"); return idx
    except ImportError:
        print("[INDEX] hnswlib unavailable, brute-force fallback")
        idx = BruteForceIndex(EMBED_DIM); idx.add_items(vecs, ids)
        bp = str(HNSW_PATH).replace(".bin", "_bf"); idx.save_index(bp)
        print(f"[INDEX] Saved {bp}.npz"); return idx

def load_index():
    try:
        import hnswlib
        if HNSW_PATH.exists():
            idx = hnswlib.Index(space="cosine", dim=EMBED_DIM)
            idx.load_index(str(HNSW_PATH)); idx.set_ef(50); return idx
    except ImportError: pass
    bp = str(HNSW_PATH).replace(".bin", "_bf.npz")
    if os.path.exists(bp):
        idx = BruteForceIndex(EMBED_DIM); idx.load_index(bp); return idx
    return None

def step_index():
    print("[INDEX] Loading embeddings...")
    ids, vecs = _load_embeddings()
    if not len(ids): print("[INDEX] No embeddings. Run --embed first."); return
    print(f"[INDEX] Building for {len(ids)} vectors...")
    build_index(ids, vecs); print("[INDEX] Done.")

# -- 3. K-MEANS PARTITIONING --
def step_partition():
    _ensure_col()
    print(f"[PARTITION] k-means k={K_PARTS}...")
    ids, vecs = _load_embeddings()
    if not len(ids): print("[PARTITION] No embeddings."); return
    rng = np.random.RandomState(42)
    centroids = vecs[rng.choice(len(ids), K_PARTS, replace=False)].copy()
    labels = np.zeros(len(ids), dtype=np.int32)
    for it in range(50):
        nl = (1.0 - vecs @ centroids.T).argmin(axis=1).astype(np.int32)
        if np.array_equal(labels, nl): print(f"[PARTITION] Converged iter {it+1}"); break
        labels = nl
        for c in range(K_PARTS):
            m = labels == c
            if m.any(): centroids[c] = _norm(vecs[m].mean(axis=0))
    with get_db() as conn:
        for s in range(0, len(ids), BATCH):
            conn.executemany("UPDATE businesses SET partition_id=? WHERE id=?",
                [(int(labels[i]), ids[i]) for i in range(s, min(s+BATCH, len(ids)))])
        conn.commit()
    u, cnt = np.unique(labels, return_counts=True)
    print(f"[PARTITION] {len(ids)} -> {len(u)} partitions")
    for p, n in sorted(zip(u, cnt), key=lambda x: -x[1]): print(f"  P{p:2d}: {n:5d}")
    print("[PARTITION] Done.")

# -- 4. BYZANTINE CONFIDENCE --
def _byz_conf(sf):
    sigs = [SOURCE_WEIGHTS[s] for s, b in SOURCE_BITS.items() if sf & (1 << b)]
    if not sigs: return 0.15
    a = np.array(sigs)
    if len(a) >= 3:
        mu, sd = a.mean(), a.std()
        if sd > 0: a = a[np.abs(a - mu) <= 2 * sd] if np.any(np.abs(a - mu) <= 2 * sd) else a
    return float(a.mean())

def step_score():
    print("[SCORE] Byzantine confidence...")
    done = 0
    with get_db() as conn:
        tot = conn.execute("SELECT count(*) FROM businesses").fetchone()[0]
        off = 0
        while off < tot:
            rows = conn.execute("SELECT id,source_flags,email,phone,owner_cluster_id "
                "FROM businesses LIMIT ? OFFSET ?", (BATCH, off)).fetchall()
            if not rows: break
            ups = []
            for r in rows:
                c = _byz_conf(r["source_flags"] or 0)
                if r["email"] and r["email"] not in ("", "None", "222"): c = min(1.0, c + 0.05)
                if r["phone"] and r["phone"] not in ("", "None"): c = min(1.0, c + 0.03)
                if r["owner_cluster_id"]: c = min(1.0, c + 0.02)
                ups.append((int(round(c * 100)), 1 if c >= 0.8 else (2 if c >= 0.55 else 3), r["id"]))
            conn.executemany("UPDATE businesses SET confidence=?,tier=? WHERE id=?", ups)
            conn.commit(); done += len(ups); off += BATCH
            sys.stdout.write(f"\r[SCORE] {done}/{tot}"); sys.stdout.flush()
    print(f"\n[SCORE] Done. {done} rescored.")
    with get_db() as conn:
        for t in (1, 2, 3):
            print(f"  Tier {t}: {conn.execute('SELECT count(*) FROM businesses WHERE tier=?',(t,)).fetchone()[0]}")

# -- 5. MCP TOOLS --
def tool_similar_businesses(p):
    bid, lim = p.get("id"), min(int(p.get("limit", 10)), 50)
    if not bid: return {"error": "id required"}
    with get_db() as conn:
        r = conn.execute("SELECT gnn_vectors FROM businesses WHERE id=?", (bid,)).fetchone()
        if not r or not r["gnn_vectors"]: return {"error": "not found or no embedding"}
        qv = np.array(json.loads(r["gnn_vectors"]), dtype=np.float32)
    idx = load_index()
    if not idx: return {"error": "Index not built. Run --index"}
    rids, _ = idx.knn_query(qv.reshape(1, -1), k=lim + 1)
    nids = [int(i) for i in rids[0] if int(i) != bid][:lim]
    with get_db() as conn:
        rows = conn.execute(f"SELECT id,name,trade,city,province,confidence,gnn_score "
            f"FROM businesses WHERE id IN ({','.join('?'*len(nids))})", nids).fetchall()
    return [dict(r) for r in rows]

def tool_list_partitions(p):
    with get_db() as conn:
        rows = conn.execute("SELECT partition_id,count(*) as cnt,"
            "GROUP_CONCAT(DISTINCT trade) as trades,GROUP_CONCAT(DISTINCT province) as provs "
            "FROM businesses WHERE partition_id IS NOT NULL GROUP BY partition_id ORDER BY cnt DESC").fetchall()
    return [{"partition_id": r["partition_id"], "count": r["cnt"],
             "top_trades": (r["trades"] or "").split(",")[:5],
             "provinces": (r["provs"] or "").split(",")} for r in rows]

def tool_search_by_embedding(p):
    vec, lim = p.get("vector", []), min(int(p.get("limit", 10)), 50)
    if len(vec) != EMBED_DIM: return {"error": f"Need {EMBED_DIM}-dim vector"}
    idx = load_index()
    if not idx: return {"error": "Index not built"}
    rids, _ = idx.knn_query(np.array(vec, dtype=np.float32).reshape(1, -1), k=lim)
    ids = [int(i) for i in rids[0]]
    with get_db() as conn:
        rows = conn.execute(f"SELECT id,name,trade,city,province,confidence,gnn_score "
            f"FROM businesses WHERE id IN ({','.join('?'*len(ids))})", ids).fetchall()
    return [dict(r) for r in rows]

def tool_get_enrichment_status(p):
    with get_db() as conn:
        t = conn.execute("SELECT count(*) FROM businesses").fetchone()[0]
        e = conn.execute("SELECT count(*) FROM businesses WHERE email IS NOT NULL AND email!='' AND email!='None'").fetchone()[0]
        v = conn.execute("SELECT count(*) FROM businesses WHERE gnn_vectors IS NOT NULL").fetchone()[0]
        pt = conn.execute("SELECT count(*) FROM businesses WHERE partition_id IS NOT NULL").fetchone()[0]
        pk = conn.execute("SELECT count(*) FROM businesses WHERE parked=1").fetchone()[0]
    return {"total": t, "with_email": e, "with_vectors": v, "with_partition": pt,
            "parked": pk, "email_pct": round(e/t*100, 1) if t else 0, "vector_pct": round(v/t*100, 1) if t else 0}

def tool_get_data_sources(p):
    src = {0: "permits", 1: "gmb", 2: "registries", 3: "yellowpages", 5: "common_crawl"}
    with get_db() as conn:
        res = [{"source": n, "bit": b, "count": conn.execute(
            "SELECT count(*) FROM businesses WHERE source_flags&?!=0", (1<<b,)).fetchone()[0]}
            for b, n in sorted(src.items())]
        tot = conn.execute("SELECT count(*) FROM businesses").fetchone()[0]
    return {"sources": res, "total_businesses": tot}

def tool_bulk_search(p):
    trades, cities = p.get("trades", []), p.get("cities", [])
    lim = min(int(p.get("limit", 25)), 100)
    parts, args = [], []
    if trades: parts.append("(" + " OR ".join(["trade LIKE ?"] * len(trades)) + ")"); args.extend(f"%{t}%" for t in trades)
    if cities: parts.append("(" + " OR ".join(["city LIKE ?"] * len(cities)) + ")"); args.extend(f"%{c}%" for c in cities)
    w = " AND ".join(parts) if parts else "1=1"; args.append(lim)
    with get_db() as conn:
        rows = conn.execute(f"SELECT id,name,trade,city,province,confidence,gnn_score "
            f"FROM businesses WHERE {w} ORDER BY confidence DESC LIMIT ?", args).fetchall()
    return [dict(r) for r in rows]

NEW_TOOLS = {
    "similar_businesses": (tool_similar_businesses, 1),
    "list_partitions": (tool_list_partitions, 0),
    "search_by_embedding": (tool_search_by_embedding, 2),
    "get_enrichment_status": (tool_get_enrichment_status, 0),
    "get_data_sources": (tool_get_data_sources, 0),
    "bulk_search": (tool_bulk_search, 0),
}

NEW_TOOL_MANIFESTS = [
    {"name": "similar_businesses", "description": "Find businesses similar to a given ID via embeddings",
     "parameters": {"id": {"type": "integer", "required": True}, "limit": {"type": "integer", "default": 10}},
     "auth": "api_key", "tier": "starter+"},
    {"name": "list_partitions", "description": "List knowledge topology partitions", "parameters": {}, "auth": "none"},
    {"name": "search_by_embedding", "description": "Search by raw 128-dim vector",
     "parameters": {"vector": {"type": "array", "required": True}, "limit": {"type": "integer", "default": 10}},
     "auth": "api_key", "tier": "contractor+"},
    {"name": "get_enrichment_status", "description": "Enrichment pipeline progress", "parameters": {}, "auth": "none"},
    {"name": "get_data_sources", "description": "Data sources with record counts", "parameters": {}, "auth": "none"},
    {"name": "bulk_search", "description": "Multi-trade/city search in one call",
     "parameters": {"trades": {"type": "array"}, "cities": {"type": "array"}, "limit": {"type": "integer", "default": 25}},
     "auth": "none"},
]

def step_patch_server():
    sp = BASE_DIR / "server.py"; content = sp.read_text()
    marker = "# --- PI INTEGRATION TOOLS ---"
    if marker in content: print("[PATCH] Already patched."); return
    anchor = "# ---------------------------------------------------------------------------\n# MCP endpoint"
    if anchor not in content: print("[PATCH] Anchor not found. Manual patch needed."); return
    content = content.replace(anchor, f"\n{marker}\nfrom pi_integration import NEW_TOOLS, NEW_TOOL_MANIFESTS\nTOOLS.update(NEW_TOOLS)\n\n{anchor}")
    sp.write_text(content); print("[PATCH] server.py patched with 6 new tools.")

def step_stats():
    with get_db() as conn:
        t = conn.execute("SELECT count(*) FROM businesses").fetchone()[0]
        wv = conn.execute("SELECT count(*) FROM businesses WHERE gnn_vectors IS NOT NULL").fetchone()[0]
        cols = {r[1] for r in conn.execute("PRAGMA table_info(businesses)")}
        wp = conn.execute("SELECT count(*) FROM businesses WHERE partition_id IS NOT NULL").fetchone()[0] if "partition_id" in cols else 0
        pc = conn.execute("SELECT count(DISTINCT partition_id) FROM businesses WHERE partition_id IS NOT NULL").fetchone()[0] if "partition_id" in cols else 0
        ag = conn.execute("SELECT avg(gnn_score) FROM businesses WHERE gnn_score>0").fetchone()[0]
        ac = conn.execute("SELECT avg(confidence) FROM businesses").fetchone()[0]
        td = {i: conn.execute("SELECT count(*) FROM businesses WHERE tier=?",(i,)).fetchone()[0] for i in (1,2,3)}
    idx = load_index()
    print("=" * 60)
    print("  CanGraph Pi Integration Stats")
    print("=" * 60)
    print(f"  Total businesses:      {t:,}")
    print(f"  With embeddings:       {wv:,} ({wv/t*100:.1f}%)")
    print(f"  With partitions:       {wp:,} ({wp/t*100:.1f}%)")
    print(f"  Avg GNN score:         {ag:.4f}" if ag else "  Avg GNN score:         N/A")
    print(f"  Avg confidence:        {ac:.1f}" if ac else "  Avg confidence:        N/A")
    print(f"  Partitions (k-means):  {pc}")
    print(f"  HNSW index:            {'loaded' if idx else 'not built'}")
    for i in (1,2,3): print(f"  Tier {i}: {td[i]:,}")
    print("=" * 60)

def main():
    ap = argparse.ArgumentParser(description="CanGraph Pi Integration")
    for f, h in [("embed","Generate embeddings"),("index","Build HNSW"),("partition","K-means"),
                  ("score","Byzantine confidence"),("patch","Patch server.py"),("all","All steps"),("stats","Stats")]:
        ap.add_argument(f"--{f}", action="store_true", help=h)
    a = ap.parse_args()
    if not any(vars(a).values()): ap.print_help(); return
    t0 = time.time()
    if a.all or a.embed: step_embed()
    if a.all or a.index: step_index()
    if a.all or a.partition: step_partition()
    if a.all or a.score: step_score()
    if a.all or a.patch: step_patch_server()
    if a.stats: step_stats()
    print(f"\nCompleted in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
