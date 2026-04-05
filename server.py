"""
CanGraph.ca — Canadian Business Intelligence MCP Server
Single-file FastAPI server with MCP protocol, Stripe billing, and landing page.
"""
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
import sqlite3
import json
import os
import hashlib
import time
from datetime import date, datetime
from typing import Optional
from contextlib import contextmanager

app = FastAPI(title="CanGraph MCP", version="1.0")

DB_PATH = os.environ.get(
    "CANGRAPH_DB",
    "/run/media/bobinzuks/82ae4087-99fd-4170-b394-3f47ac90dca1/projects/projects/"
    "money-now/autoreply-pro/email-velocity/data/canada_b2b.db",
)

TIERS = {
    "starter": {"limit": 1000, "rank": 1, "price": "$99/mo"},
    "contractor": {"limit": 5000, "rank": 2, "price": "$249/mo"},
    "pro": {"limit": 10000, "rank": 3, "price": "$499/mo"},
    "corporate": {"limit": 0, "rank": 4, "price": "$999/mo"},  # 0 = unlimited
}

TIER_FEATURES = {
    "starter": [
        "1,000 API calls/day",
        "Business search by trade/city/province",
        "Contact info (email + phone)",
        "GNN confidence scores",
    ],
    "contractor": [
        "5,000 API calls/day",
        "Everything in Starter",
        "Trades + permit data",
        "Category filtering",
    ],
    "pro": [
        "10,000 API calls/day",
        "Everything in Contractor",
        "Owner cluster analysis",
        "Sibling domain discovery",
    ],
    "corporate": [
        "Unlimited API calls",
        "Everything in Pro",
        "Bulk CSV export",
        "Webhook notifications",
        "Priority support",
    ],
}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                key TEXT PRIMARY KEY,
                tier TEXT NOT NULL,
                email TEXT,
                req_today INTEGER DEFAULT 0,
                req_date TEXT DEFAULT (date('now')),
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()


@app.on_event("startup")
def on_startup():
    init_db()


# ---------------------------------------------------------------------------
# Auth + rate limiting
# ---------------------------------------------------------------------------

def validate_key(api_key: Optional[str], required_tier_rank: int = 0):
    """Validate API key and enforce rate limits. Returns tier name or raises."""
    if not api_key:
        if required_tier_rank > 0:
            raise HTTPException(401, detail="X-API-Key header required")
        return None

    with get_db() as conn:
        row = conn.execute(
            "SELECT key, tier, req_today, req_date FROM api_keys WHERE key = ?",
            (api_key,),
        ).fetchone()

    if not row:
        raise HTTPException(403, detail="Invalid API key")

    tier = row["tier"]
    tier_info = TIERS.get(tier)
    if not tier_info:
        raise HTTPException(403, detail="Unknown tier")

    if tier_info["rank"] < required_tier_rank:
        raise HTTPException(
            403,
            detail=f"This tool requires tier rank {required_tier_rank}+. "
            f"Your tier '{tier}' (rank {tier_info['rank']}) is insufficient.",
        )

    today = date.today().isoformat()
    req_today = row["req_today"]
    req_date = row["req_date"]

    if req_date != today:
        req_today = 0

    limit = tier_info["limit"]
    if limit > 0 and req_today >= limit:
        raise HTTPException(429, detail=f"Rate limit exceeded ({limit}/day for {tier})")

    with get_db() as conn:
        conn.execute(
            "UPDATE api_keys SET req_today = ?, req_date = ? WHERE key = ?",
            (req_today + 1, today, api_key),
        )
        conn.commit()

    return tier


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

def tool_search_businesses(params: dict):
    trade = params.get("trade", "")
    city = params.get("city", "")
    province = params.get("province", "")
    category = params.get("category", "")
    limit = min(int(params.get("limit", 25)), 100)

    clauses, args = [], []
    if trade:
        clauses.append("trade LIKE ?")
        args.append(f"%{trade}%")
    if city:
        clauses.append("city LIKE ?")
        args.append(f"%{city}%")
    if province:
        clauses.append("province LIKE ?")
        args.append(f"%{province}%")
    if category:
        clauses.append("category = ?")
        args.append(category)

    where = " AND ".join(clauses) if clauses else "1=1"
    sql = (
        f"SELECT id, name, trade, city, province, domain, category, "
        f"confidence, gnn_score FROM businesses WHERE {where} "
        f"ORDER BY confidence DESC LIMIT ?"
    )
    args.append(limit)

    with get_db() as conn:
        rows = conn.execute(sql, args).fetchall()

    return [dict(r) for r in rows]


def tool_get_business(params: dict):
    bid = params.get("id")
    if not bid:
        raise HTTPException(400, detail="id is required")

    with get_db() as conn:
        row = conn.execute(
            "SELECT id, name, trade, city, province, domain, website, category, "
            "confidence, gnn_score, owner_cluster_id, source_flags, "
            "created_at, updated_at FROM businesses WHERE id = ?",
            (bid,),
        ).fetchone()

    if not row:
        raise HTTPException(404, detail="Business not found")
    return dict(row)


def tool_get_contacts(params: dict):
    bid = params.get("business_id")
    if not bid:
        raise HTTPException(400, detail="business_id is required")

    with get_db() as conn:
        row = conn.execute(
            "SELECT id, name, email, phone, contact_quality_score "
            "FROM businesses WHERE id = ?",
            (bid,),
        ).fetchone()

    if not row:
        raise HTTPException(404, detail="Business not found")
    return dict(row)


def tool_find_cluster(params: dict):
    domain = params.get("domain", "")
    if not domain:
        raise HTTPException(400, detail="domain is required")

    with get_db() as conn:
        biz = conn.execute(
            "SELECT owner_cluster_id FROM businesses WHERE domain = ?", (domain,)
        ).fetchone()

        if not biz or not biz["owner_cluster_id"]:
            return {"cluster": None, "siblings": []}

        cluster_id = biz["owner_cluster_id"]
        cluster = conn.execute(
            "SELECT * FROM clusters WHERE cluster_id = ?", (cluster_id,)
        ).fetchone()

        siblings = conn.execute(
            "SELECT id, name, domain, trade, city FROM businesses "
            "WHERE owner_cluster_id = ? AND domain != ?",
            (cluster_id, domain),
        ).fetchall()

    result = {
        "cluster_id": cluster_id,
        "cluster": dict(cluster) if cluster else None,
        "siblings": [dict(s) for s in siblings],
    }
    return result


def tool_get_stats():
    with get_db() as conn:
        c = conn.cursor()
        total = c.execute("SELECT count(*) FROM businesses").fetchone()[0]
        phones = c.execute(
            "SELECT count(*) FROM businesses WHERE phone IS NOT NULL AND phone != ''"
        ).fetchone()[0]
        emails = c.execute(
            "SELECT count(*) FROM businesses WHERE email IS NOT NULL AND email != ''"
        ).fetchone()[0]
        clusters = c.execute("SELECT count(*) FROM clusters").fetchone()[0]
        categories = [
            r[0]
            for r in c.execute(
                "SELECT DISTINCT category FROM businesses ORDER BY category"
            ).fetchall()
        ]
    return {
        "total_businesses": total,
        "with_phone": phones,
        "with_email": emails,
        "clusters": clusters,
        "categories": categories,
    }


def tool_list_categories():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT category, count(*) as cnt FROM businesses "
            "GROUP BY category ORDER BY cnt DESC"
        ).fetchall()
    return [{"category": r["category"], "count": r["cnt"]} for r in rows]


# Tool registry: name -> (handler, required_tier_rank)
TOOLS = {
    "search_businesses": (tool_search_businesses, 0),
    "get_business": (tool_get_business, 0),
    "get_contacts": (tool_get_contacts, 1),  # starter+
    "find_cluster": (tool_find_cluster, 3),  # pro+
    "get_stats": (lambda _: tool_get_stats(), 0),
    "list_categories": (lambda _: tool_list_categories(), 0),
}



# --- PI INTEGRATION TOOLS ---
from pi_integration import NEW_TOOLS, NEW_TOOL_MANIFESTS
TOOLS.update(NEW_TOOLS)

# ---------------------------------------------------------------------------
# MCP endpoint
# ---------------------------------------------------------------------------

@app.post("/mcp")
async def mcp_endpoint(request: Request, x_api_key: Optional[str] = Header(None)):
    body = await request.json()
    tool_name = body.get("tool", "")
    tool_input = body.get("input", {})

    if tool_name not in TOOLS:
        return JSONResponse(
            {"error": f"Unknown tool: {tool_name}", "available": list(TOOLS.keys())},
            status_code=400,
        )

    handler, required_rank = TOOLS[tool_name]
    validate_key(x_api_key, required_rank)

    try:
        result = handler(tool_input)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

    return JSONResponse({
        "type": "tool_result",
        "tool": tool_name,
        "content": [{"type": "json", "data": result}],
    })


# ---------------------------------------------------------------------------
# MCP manifest
# ---------------------------------------------------------------------------

@app.get("/.well-known/mcp.json")
def mcp_manifest():
    return {
        "name": "CanGraph",
        "version": "1.0",
        "description": "Canadian Business Intelligence MCP — 44K+ verified businesses",
        "tools": [
            {
                "name": "search_businesses",
                "description": "Search Canadian businesses by trade, city, province, category",
                "parameters": {
                    "trade": {"type": "string", "description": "Trade/industry keyword"},
                    "city": {"type": "string", "description": "City name"},
                    "province": {"type": "string", "description": "Province code (ON, BC, AB, etc.)"},
                    "category": {"type": "string", "description": "Business category"},
                    "limit": {"type": "integer", "description": "Max results (1-100)", "default": 25},
                },
                "auth": "none",
            },
            {
                "name": "get_business",
                "description": "Get full details for a single business by ID",
                "parameters": {
                    "id": {"type": "integer", "description": "Business ID", "required": True},
                },
                "auth": "none",
            },
            {
                "name": "get_contacts",
                "description": "Get email and phone for a business (Starter+ tier required)",
                "parameters": {
                    "business_id": {"type": "integer", "description": "Business ID", "required": True},
                },
                "auth": "api_key",
                "tier": "starter+",
            },
            {
                "name": "find_cluster",
                "description": "Find owner cluster and sibling domains for a domain (Pro+ tier)",
                "parameters": {
                    "domain": {"type": "string", "description": "Domain to look up", "required": True},
                },
                "auth": "api_key",
                "tier": "pro+",
            },
            {
                "name": "get_stats",
                "description": "Get database statistics (public, no key needed)",
                "parameters": {},
                "auth": "none",
            },
            {
                "name": "list_categories",
                "description": "List all business categories with counts (public)",
                "parameters": {},
                "auth": "none",
            },
        ] + NEW_TOOL_MANIFESTS,
        "auth": {"type": "api_key", "header": "X-API-Key"},
        "endpoint": "https://cangraph.ca/mcp",
    }


# ---------------------------------------------------------------------------
# Stripe integration
# ---------------------------------------------------------------------------

@app.get("/pay")
async def pay(tier: str = "starter"):
    if tier not in TIERS:
        raise HTTPException(400, detail=f"Invalid tier. Choose: {list(TIERS.keys())}")

    stripe_key = os.environ.get("STRIPE_SECRET_KEY")
    if not stripe_key:
        return JSONResponse({
            "status": "stripe_not_configured",
            "message": "Set STRIPE_SECRET_KEY to enable payments",
            "tier": tier,
            "price": TIERS[tier]["price"],
            "features": TIER_FEATURES[tier],
        })

    price_id = os.environ.get(f"STRIPE_PRICE_{tier.upper()}")
    if not price_id:
        raise HTTPException(500, detail=f"No Stripe price configured for tier: {tier}")

    try:
        import stripe
        stripe.api_key = stripe_key
        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": price_id, "quantity": 1}],
            success_url="https://cangraph.ca/success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url="https://cangraph.ca/",
            metadata={"tier": tier},
        )
        return JSONResponse({
            "checkout_url": session.url,
            "tier": tier,
            "price": TIERS[tier]["price"],
            "features": TIER_FEATURES[tier],
        })
    except Exception as e:
        raise HTTPException(500, detail=f"Stripe error: {str(e)}")


@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    stripe_secret = os.environ.get("STRIPE_SECRET_KEY")
    webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET")

    if not stripe_secret or not webhook_secret:
        return JSONResponse({"status": "stripe_not_configured"}, status_code=200)

    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")

    try:
        import stripe
        stripe.api_key = stripe_secret
        event = stripe.Webhook.construct_event(payload, sig, webhook_secret)
    except Exception as e:
        raise HTTPException(400, detail=f"Webhook verification failed: {str(e)}")

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        email = session.get("customer_email", session.get("customer_details", {}).get("email", ""))
        tier = session.get("metadata", {}).get("tier", "starter")

        secret = os.environ.get("CANGRAPH_KEY_SECRET", "cangraph-default-secret")
        raw = f"{email}{time.time()}{secret}"
        api_key = hashlib.sha256(raw.encode()).hexdigest()[:32]

        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO api_keys (key, tier, email) VALUES (?, ?, ?)",
                (api_key, tier, email),
            )
            conn.commit()

        return JSONResponse({
            "status": "ok",
            "api_key": api_key,
            "tier": tier,
            "email": email,
        })

    return JSONResponse({"status": "ignored", "event_type": event["type"]})


# ---------------------------------------------------------------------------
# Landing page
# ---------------------------------------------------------------------------

LANDING_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CanGraph.ca — Canadian Business Intelligence MCP</title>
<style>
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
html{scroll-behavior:smooth}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
background:#0a0a0f;color:#e0e0e0;line-height:1.7}
a{color:#4fc3f7;text-decoration:none;transition:color .2s}
a:hover{color:#81d4fa}
.wrap{max-width:1140px;margin:0 auto;padding:0 28px}

/* --- Header --- */
header{position:sticky;top:0;z-index:100;padding:16px 0;
background:rgba(10,10,15,.72);backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px);
border-bottom:1px solid rgba(255,255,255,.06)}
header .wrap{display:flex;justify-content:space-between;align-items:center}
.logo{font-size:1.35rem;font-weight:800;color:#fff;letter-spacing:-.02em}
.logo span{color:#ef5350}
nav a{margin-left:28px;font-size:.88rem;color:#8a8a9a;font-weight:500;transition:color .2s}
nav a:hover{color:#fff}

/* --- Hero --- */
.hero{position:relative;padding:120px 0 80px;text-align:center;overflow:hidden}
.hero::before{content:'';position:absolute;inset:0;
background:radial-gradient(ellipse 80% 60% at 50% 0%,rgba(79,195,247,.12),transparent 60%),
radial-gradient(ellipse 60% 50% at 80% 20%,rgba(255,107,107,.07),transparent 50%);
pointer-events:none;animation:heroShift 12s ease-in-out infinite alternate}
@keyframes heroShift{0%{opacity:.8;transform:scale(1)}100%{opacity:1;transform:scale(1.05)}}
.hero h1{font-size:3.4rem;font-weight:800;color:#fff;letter-spacing:-.03em;
line-height:1.15;margin-bottom:20px;position:relative}
.hero h1 em{font-style:normal;color:#4fc3f7}
.hero .sub{font-size:1.15rem;color:#8a8a9a;max-width:560px;margin:0 auto 56px;position:relative}

/* --- Stats --- */
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:20px;margin-bottom:0;position:relative}
.stat{background:rgba(18,18,31,.65);border:1px solid rgba(79,195,247,.1);
border-radius:14px;padding:28px 16px;text-align:center;
backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);transition:border-color .3s,box-shadow .3s}
.stat:hover{border-color:rgba(79,195,247,.35);box-shadow:0 0 24px rgba(79,195,247,.08)}
.stat .num{font-size:2.2rem;font-weight:700;color:#4fc3f7;
animation:pulse 3s ease-in-out infinite}
@keyframes pulse{0%,100%{text-shadow:0 0 8px rgba(79,195,247,.15)}50%{text-shadow:0 0 20px rgba(79,195,247,.35)}}
.stat .label{font-size:.82rem;color:#6a6a7a;margin-top:4px;text-transform:uppercase;letter-spacing:.06em}

/* --- Sections --- */
.section{padding:80px 0}
.section-title{font-size:1.9rem;font-weight:700;color:#fff;margin-bottom:12px;text-align:center;letter-spacing:-.02em}
.section-sub{text-align:center;color:#6a6a7a;margin-bottom:48px;font-size:1rem}

/* --- Data Sources --- */
.sources{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
.source{background:rgba(18,18,31,.5);border:1px solid rgba(255,255,255,.06);
border-radius:12px;padding:22px;backdrop-filter:blur(6px);transition:border-color .3s,transform .25s}
.source:hover{border-color:rgba(79,195,247,.25);transform:translateY(-2px)}
.source .ico{font-size:1.4rem;margin-bottom:8px}
.source h4{color:#e0e0e0;font-size:.92rem;margin-bottom:4px}
.source p{color:#6a6a7a;font-size:.8rem;line-height:1.5}

/* --- Steps --- */
.steps{display:grid;grid-template-columns:repeat(3,1fr);gap:28px;counter-reset:step}
.step{position:relative;background:rgba(18,18,31,.5);border:1px solid rgba(255,255,255,.06);
border-radius:14px;padding:32px 24px 28px;text-align:center;counter-increment:step;transition:border-color .3s}
.step:hover{border-color:rgba(79,195,247,.25)}
.step::before{content:counter(step);display:inline-flex;align-items:center;justify-content:center;
width:36px;height:36px;border-radius:50%;background:rgba(79,195,247,.12);color:#4fc3f7;
font-weight:700;font-size:.95rem;margin-bottom:16px}
.step h4{color:#fff;font-size:1rem;margin-bottom:8px}
.step p{color:#6a6a7a;font-size:.85rem}

/* --- Pricing --- */
.pricing{display:grid;grid-template-columns:repeat(4,1fr);gap:18px}
.card{background:rgba(18,18,31,.6);border:1px solid rgba(255,255,255,.06);border-radius:16px;
padding:30px 24px;position:relative;backdrop-filter:blur(8px);transition:border-color .3s,box-shadow .3s}
.card:hover{border-color:rgba(79,195,247,.3);box-shadow:0 0 32px rgba(79,195,247,.06)}
.card.pop{border-color:rgba(79,195,247,.4)}
.card.pop::after{content:'POPULAR';position:absolute;top:-11px;right:18px;
background:linear-gradient(135deg,#4fc3f7,#29b6f6);color:#000;font-size:.65rem;
font-weight:700;padding:3px 12px;border-radius:6px;letter-spacing:.04em}
.card h3{font-size:1.05rem;color:#fff;margin-bottom:6px;font-weight:600}
.card .price{font-size:2rem;font-weight:700;color:#4fc3f7;margin-bottom:18px}
.card .price small{font-size:.8rem;color:#6a6a7a;font-weight:400}
.card ul{list-style:none;margin-bottom:22px}
.card li{padding:5px 0;font-size:.84rem;color:#9a9aaa}
.card li::before{content:'\\2713 ';color:#4fc3f7;font-weight:600}
.btn-disabled{display:inline-block;padding:10px 0;width:100%;text-align:center;
border-radius:8px;font-weight:600;font-size:.85rem;
background:rgba(255,255,255,.04);color:#4a4a5a;border:1px solid rgba(255,255,255,.06);cursor:default}

/* --- Agents --- */
.agent-box{background:rgba(13,13,24,.8);border:1px solid rgba(255,255,255,.06);
border-radius:16px;padding:44px;backdrop-filter:blur(8px)}
.agent-box h3{color:#fff;font-size:1.3rem;margin-bottom:20px;font-weight:700}
.copy-row{display:flex;align-items:center;gap:8px;margin-bottom:20px}
.copy-url{flex:1;background:rgba(26,26,46,.8);border:1px solid rgba(79,195,247,.15);
border-radius:8px;padding:12px 16px;font-family:'SF Mono',Monaco,Consolas,monospace;
font-size:.82rem;color:#a5d6a7;overflow-x:auto;white-space:nowrap}
.copy-btn{background:rgba(79,195,247,.12);border:1px solid rgba(79,195,247,.25);
color:#4fc3f7;padding:12px 18px;border-radius:8px;cursor:pointer;font-size:.8rem;
font-weight:600;transition:background .2s;white-space:nowrap}
.copy-btn:hover{background:rgba(79,195,247,.2)}
.agent-box .compat{color:#6a6a7a;font-size:.9rem;margin-bottom:24px}
.agent-box .compat strong{color:#9a9aaa}
.code-block{background:rgba(13,13,24,.9);border:1px solid rgba(255,255,255,.08);
border-radius:10px;padding:20px 24px;font-family:'SF Mono',Monaco,Consolas,monospace;
font-size:.8rem;color:#a5d6a7;overflow-x:auto;white-space:pre;line-height:1.65}
.code-block .k{color:#c792ea}.code-block .s{color:#c3e88d}.code-block .c{color:#545468}

/* --- API Docs --- */
.api-table{width:100%;border-collapse:collapse}
.api-table th{text-align:left;padding:12px 16px;font-size:.78rem;color:#6a6a7a;
text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid rgba(255,255,255,.06)}
.api-table td{padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.04);font-size:.88rem}
.api-table tr:hover td{background:rgba(79,195,247,.03)}
.api-table .tool{color:#4fc3f7;font-family:'SF Mono',Monaco,Consolas,monospace;font-size:.82rem;font-weight:600}
.api-table .auth{font-size:.75rem;padding:3px 10px;border-radius:4px;font-weight:600}
.auth-none{background:rgba(76,175,80,.12);color:#81c784}
.auth-key{background:rgba(255,183,77,.12);color:#ffb74d}

/* --- Footer --- */
footer{padding:44px 0;border-top:1px solid rgba(255,255,255,.05);text-align:center;color:#3a3a4a;font-size:.82rem}
footer a{color:#4a4a5a;margin:0 14px}
footer a:hover{color:#8a8a9a}

/* --- Scroll reveal --- */
.reveal{opacity:0;transform:translateY(24px);transition:opacity .7s ease,transform .7s ease}
.reveal.visible{opacity:1;transform:translateY(0)}

/* --- Responsive --- */
@media(max-width:960px){
.pricing{grid-template-columns:repeat(2,1fr)}
.sources{grid-template-columns:repeat(2,1fr)}
}
@media(max-width:640px){
.hero h1{font-size:2.2rem}
.stats{grid-template-columns:repeat(2,1fr)}
.pricing{grid-template-columns:1fr}
.steps{grid-template-columns:1fr}
.sources{grid-template-columns:1fr}
.agent-box{padding:28px 20px}
nav a{margin-left:16px;font-size:.8rem}
}
</style>
</head>
<body>

<!-- Header -->
<header>
<div class="wrap">
<div class="logo">Can<span>Graph</span>.ca</div>
<nav>
<a href="#pricing">Pricing</a>
<a href="#agents">For Agents</a>
<a href="#api-docs">API Docs</a>
</nav>
</div>
</header>

<!-- Hero -->
<section class="hero">
<div class="wrap">
<h1>Canadian Business Intelligence<br><em>for AI Agents</em></h1>
<p class="sub">MCP-native access to verified Canadian businesses, contacts, and owner intelligence. Built for agents that need real data.</p>

<div class="stats reveal">
<div class="stat"><div class="num">{total_businesses:,}</div><div class="label">Businesses</div></div>
<div class="stat"><div class="num">{with_phone:,}</div><div class="label">Phone Numbers</div></div>
<div class="stat"><div class="num">{with_email:,}</div><div class="label">Email Addresses</div></div>
<div class="stat"><div class="num">{clusters:,}</div><div class="label">Owner Clusters</div></div>
</div>
</div>
</section>

<!-- Data Sources -->
<section class="section">
<div class="wrap">
<h2 class="section-title reveal">Built on Verified Public Data</h2>
<p class="section-sub reveal">Six layers of sourcing, cross-referenced and confidence-scored</p>
<div class="sources reveal">
<div class="source"><div class="ico">&#127760;</div><h4>Common Crawl</h4><p>60M+ Canadian pages indexed and entity-extracted</p></div>
<div class="source"><div class="ico">&#127959;</div><h4>Provincial Building Permits</h4><p>Licensed contractor records from public registries</p></div>
<div class="source"><div class="ico">&#128205;</div><h4>Google Maps Places API</h4><p>Location, hours, and category data for validation</p></div>
<div class="source"><div class="ico">&#128221;</div><h4>Licensed Contractor Registries</h4><p>Provincial trade license verification databases</p></div>
<div class="source"><div class="ico">&#127961;</div><h4>Municipal Open Data</h4><p>City-level business license and permit datasets</p></div>
<div class="source"><div class="ico">&#129516;</div><h4>GNN Confidence Scoring</h4><p>Graph neural network deduplication and scoring</p></div>
</div>
</div>
</section>

<!-- How It Works -->
<section class="section" style="padding-top:0">
<div class="wrap">
<h2 class="section-title reveal">How It Works</h2>
<p class="section-sub reveal">Three steps from zero to verified contacts</p>
<div class="steps reveal">
<div class="step"><h4>Connect</h4><p>Point your agent at our MCP manifest endpoint</p></div>
<div class="step"><h4>Search</h4><p>Query by trade, city, province, or category</p></div>
<div class="step"><h4>Get Results</h4><p>Receive verified contacts with confidence scores</p></div>
</div>
</div>
</section>

<!-- Pricing -->
<section class="section" id="pricing">
<div class="wrap">
<h2 class="section-title reveal">Pricing</h2>
<p class="section-sub reveal">Simple, predictable plans for every scale</p>
<div class="pricing reveal">

<div class="card">
<h3>Starter</h3>
<div class="price">$99<small>/mo</small></div>
<ul>
<li>1,000 API calls/day</li>
<li>Business search</li>
<li>Contact info (email + phone)</li>
<li>GNN confidence scores</li>
</ul>
<span class="btn-disabled">Coming Soon</span>
</div>

<div class="card">
<h3>Contractor</h3>
<div class="price">$199<small>/mo</small></div>
<ul>
<li>5,000 API calls/day</li>
<li>Everything in Starter</li>
<li>Trades + permit data</li>
<li>Category filtering</li>
</ul>
<span class="btn-disabled">Coming Soon</span>
</div>

<div class="card pop">
<h3>Pro</h3>
<div class="price">$299<small>/mo</small></div>
<ul>
<li>10,000 API calls/day</li>
<li>Everything in Contractor</li>
<li>Owner cluster analysis</li>
<li>Sibling domain discovery</li>
</ul>
<span class="btn-disabled">Coming Soon</span>
</div>

<div class="card">
<h3>Corporate</h3>
<div class="price">$999<small>/mo</small></div>
<ul>
<li>Unlimited API calls</li>
<li>Everything in Pro</li>
<li>Bulk CSV export</li>
<li>Webhook notifications</li>
<li>Priority support</li>
</ul>
<span class="btn-disabled">Coming Soon</span>
</div>

</div>
</div>
</section>

<!-- For Agents -->
<section class="section" id="agents">
<div class="wrap">
<div class="agent-box reveal">
<h3>Built for AI Agents</h3>
<div class="copy-row">
<div class="copy-url">https://cangraph.ca/.well-known/mcp.json</div>
<button class="copy-btn" onclick="navigator.clipboard.writeText('https://cangraph.ca/.well-known/mcp.json').then(function(){event.target.textContent='Copied!'})">Copy</button>
</div>
<p class="compat">Works with <strong>Claude, GPT, LangChain, CrewAI</strong>, and any MCP-compatible agent framework.</p>
<div class="code-block"><span class="c">// POST https://cangraph.ca/mcp</span>
{
  <span class="k">"tool"</span>: <span class="s">"search_businesses"</span>,
  <span class="k">"input"</span>: {
    <span class="k">"trade"</span>: <span class="s">"plumber"</span>,
    <span class="k">"city"</span>: <span class="s">"Toronto"</span>,
    <span class="k">"province"</span>: <span class="s">"ON"</span>,
    <span class="k">"limit"</span>: 10
  }
}</div>
</div>
</div>
</section>

<!-- API Docs -->
<section class="section" id="api-docs">
<div class="wrap">
<h2 class="section-title reveal">API Reference</h2>
<p class="section-sub reveal">All tools available via the MCP protocol endpoint</p>
<div class="reveal" style="overflow-x:auto">
<table class="api-table">
<thead><tr><th>Tool</th><th>Description</th><th>Auth</th></tr></thead>
<tbody>
<tr><td class="tool">search_businesses</td><td>Search by trade, city, province, or category</td><td><span class="auth auth-none">Public</span></td></tr>
<tr><td class="tool">get_business</td><td>Full details for a single business by ID</td><td><span class="auth auth-none">Public</span></td></tr>
<tr><td class="tool">get_contacts</td><td>Email and phone for a business</td><td><span class="auth auth-key">Starter+</span></td></tr>
<tr><td class="tool">find_cluster</td><td>Owner cluster and sibling domains</td><td><span class="auth auth-key">Pro+</span></td></tr>
<tr><td class="tool">get_stats</td><td>Database statistics and category list</td><td><span class="auth auth-none">Public</span></td></tr>
<tr><td class="tool">list_categories</td><td>All business categories with counts</td><td><span class="auth auth-none">Public</span></td></tr>
</tbody>
</table>
</div>
</div>
</section>

<!-- Footer -->
<footer>
<div class="wrap">
<p style="margin-bottom:10px">&copy; {year} CanGraph.ca</p>
<a href="#api-docs">API Docs</a>
<a href="#">Status</a>
<a href="#">GitHub</a>
</div>
</footer>

<script>
(function(){var els=document.querySelectorAll('.reveal');if(!('IntersectionObserver' in window)){els.forEach(function(e){e.classList.add('visible')});return}var obs=new IntersectionObserver(function(entries){entries.forEach(function(e){if(e.isIntersecting){e.target.classList.add('visible');obs.unobserve(e.target)}})},{threshold:0.12});els.forEach(function(e){obs.observe(e)})})();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def landing():
    stats = tool_get_stats()
    tb = f"{stats['total_businesses']:,}"
    wp = f"{stats['with_phone']:,}"
    we = f"{stats['with_email']:,}"
    cl = f"{stats['clusters']:,}"
    yr = str(datetime.now().year)
    html = LANDING_HTML
    html = html.replace("{total_businesses:,}", tb).replace("{total_businesses}", tb)
    html = html.replace("{with_phone:,}", wp).replace("{with_phone}", wp)
    html = html.replace("{with_email:,}", we).replace("{with_email}", we)
    html = html.replace("{clusters:,}", cl).replace("{clusters}", cl)
    html = html.replace("{year}", yr)
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    try:
        stats = tool_get_stats()
        return {"status": "ok", "businesses": stats["total_businesses"]}
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=500)
