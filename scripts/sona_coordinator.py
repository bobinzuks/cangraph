#!/usr/bin/env python3
"""
SONA Resource Coordinator — Distributes work across VPS + GitHub + Local PC.

Monitors load, success rates, and throughput per environment.
Automatically shifts work to wherever has capacity.

SONA principles:
  - Each environment has learned rate limits
  - Success → speed up, failure → back off
  - Rebalance every check cycle
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

STATE_FILE = Path("data/sona_coordinator.json")

DEFAULT_STATE = {
    "vps": {
        "host": "root@147.93.113.37",
        "max_load": 0.6,  # CPU target (VPS is throttled)
        "tasks": ["email_harvest", "yp_scraper", "web_mapper"],
        "throughput": 0, "errors": 0, "last_check": None,
        "health": "unknown"
    },
    "github": {
        "max_concurrent": 20,
        "tasks": ["wayback_harvest", "tracking_recrawl", "email_harvest_gh", "yp_scraper_gh"],
        "throughput": 0, "errors": 0, "last_check": None,
        "health": "unknown"
    },
    "local": {
        "max_workers": 20,
        "tasks": ["email_harvest_local", "registry_scraper", "data_merge"],
        "throughput": 0, "errors": 0, "last_check": None,
        "health": "unknown"
    }
}


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return json.loads(json.dumps(DEFAULT_STATE))


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def run(cmd, timeout=30):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip()
    except Exception as e:
        return -1, str(e)


def check_vps(state):
    """Check VPS health and current workload."""
    code, out = run(
        "nix-shell -p sshpass --run \"sshpass -p '/Pz8Ls)mVHC3v5F' "
        "ssh -o ConnectTimeout=15 -o StrictHostKeyChecking=no root@147.93.113.37 "
        "'echo OK && pgrep -afc yellowpages 2>/dev/null; pgrep -afc email_harvest 2>/dev/null; "
        "pgrep -afc web-mapper 2>/dev/null; "
        "cat /proc/loadavg 2>/dev/null'\"", timeout=30)

    if code != 0 or "OK" not in out:
        state["vps"]["health"] = "unreachable"
        return

    lines = out.strip().split("\n")
    state["vps"]["health"] = "ok"
    state["vps"]["last_check"] = datetime.utcnow().isoformat()

    # Parse load average
    for line in lines:
        if "." in line and len(line.split()) >= 3:
            try:
                load1 = float(line.split()[0])
                state["vps"]["current_load"] = load1
            except:
                pass


def check_github(state):
    """Check GitHub Actions status."""
    code, out = run("gh run list -R bobinzuks/cangraph --limit 10 --json status,name,conclusion 2>/dev/null", timeout=15)
    if code != 0:
        state["github"]["health"] = "error"
        return

    try:
        runs = json.loads(out)
        active = sum(1 for r in runs if r.get("status") == "in_progress")
        failed = sum(1 for r in runs if r.get("conclusion") == "failure")
        state["github"]["health"] = "ok"
        state["github"]["active_runs"] = active
        state["github"]["recent_failures"] = failed
        state["github"]["last_check"] = datetime.utcnow().isoformat()
    except:
        state["github"]["health"] = "parse_error"


def check_local(state):
    """Check local PC resources."""
    code, out = run("cat /proc/loadavg", timeout=5)
    if code == 0:
        try:
            load1 = float(out.split()[0])
            state["local"]["health"] = "ok"
            state["local"]["current_load"] = load1
            state["local"]["last_check"] = datetime.utcnow().isoformat()
        except:
            state["local"]["health"] = "error"

    # Count local workers
    code2, out2 = run("pgrep -afc 'email_harvest\\|registry_sona\\|wayback_harvest' 2>/dev/null || echo 0", timeout=5)
    try:
        state["local"]["active_workers"] = int(out2.strip())
    except:
        state["local"]["active_workers"] = 0


def get_db_stats():
    """Get current DB counts from VPS."""
    code, out = run(
        "nix-shell -p sshpass --run \"sshpass -p '/Pz8Ls)mVHC3v5F' "
        "ssh -o ConnectTimeout=15 -o StrictHostKeyChecking=no root@147.93.113.37 "
        "'sqlite3 /opt/cangraph/canada_b2b.db "
        "\\\"SELECT \\\\\\\"businesses:\\\\\\\" || count(*) FROM businesses; "
        "SELECT \\\\\\\"yp:\\\\\\\" || count(*) FROM yp_listings; "
        "SELECT \\\\\\\"emails:\\\\\\\" || count(*) FROM harvested_emails;\\\"'\"",
        timeout=30)
    return out if code == 0 else "DB check failed"


def print_status(state):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    print(f"\n{'='*60}")
    print(f"  SONA COORDINATOR STATUS — {ts}")
    print(f"{'='*60}")

    for env in ["vps", "github", "local"]:
        e = state[env]
        health = e.get("health", "?")
        icon = "✓" if health == "ok" else "✗" if health in ("unreachable", "error") else "?"
        print(f"\n  [{icon}] {env.upper()}: {health}")
        if "current_load" in e:
            print(f"      Load: {e['current_load']:.2f}")
        if "active_runs" in e:
            print(f"      Active GH runs: {e['active_runs']}")
        if "active_workers" in e:
            print(f"      Local workers: {e['active_workers']}")


def main():
    state = load_state()

    print("SONA Coordinator — checking all environments...")

    check_vps(state)
    check_github(state)
    check_local(state)

    print_status(state)

    db = get_db_stats()
    print(f"\n  DB: {db}")

    save_state(state)

    # Recommendations
    print(f"\n{'='*60}")
    print("  RECOMMENDATIONS")
    print(f"{'='*60}")

    vps_ok = state["vps"].get("health") == "ok"
    gh_ok = state["github"].get("health") == "ok"
    local_ok = state["local"].get("health") == "ok"
    local_load = state["local"].get("current_load", 0)
    gh_active = state["github"].get("active_runs", 0)

    if local_ok and local_load < 4.0:
        avail = max(1, int(20 - local_load * 2))
        print(f"\n  LOCAL: {avail} workers available")
        print(f"    -> Launch: email_harvest (--workers {avail} --limit 20000)")
        print(f"    -> Launch: registry_sona (--registry all)")

    if gh_ok and gh_active < 15:
        avail = 20 - gh_active
        print(f"\n  GITHUB: {avail} runner slots free")
        print(f"    -> Launch: wayback-harvest (if not running)")
        print(f"    -> Launch: email harvest GH workflow")

    if vps_ok:
        print(f"\n  VPS: Keep current tasks (YP + email + web-mapper)")
        print(f"    -> Don't add more — CPU throttled")

    print(f"\n{'='*60}")


if __name__ == "__main__":
    main()
