#!/usr/bin/env python3
"""
scripts/verify_sprint3.py
===========================
Verifies Sprint 3 deliverables:
  1. Delta Lake Silver has rows
  2. gold.stock_summary has rows with non-null RSI
  3. gold.v_market_pulse returns data
  4. gold.v_latest_prices returns data

Exit code 0 = all checks pass.
"""

import os
import sys
import time
import subprocess

GREEN = "\033[92m"
RED   = "\033[91m"
RESET = "\033[0m"


def _psql(query: str) -> str:
    result = subprocess.run(
        [
            "docker", "exec", "finscope_postgres",
            "psql", "-U", "finscope_admin", "-d", "finscope",
            "-t", "-c", query,
        ],
        capture_output=True, text=True, timeout=15,
    )
    return result.stdout.strip()


def check_silver_delta():
    result = subprocess.run(
        [
            "docker", "exec", "finscope_spark_master",
            "find", "/opt/delta-lake/silver", "-name", "*.parquet",
        ],
        capture_output=True, text=True, timeout=15,
    )
    files = [l for l in result.stdout.strip().split("\n") if l]
    if files:
        return True, f"{len(files)} parquet file(s) in Silver Delta Lake"
    return False, "No Silver parquet files"


def check_gold_summary():
    out = _psql("SELECT COUNT(*) FROM gold.stock_summary WHERE rsi_14 IS NOT NULL;")
    try:
        count = int(out.split()[0])
        if count > 0:
            return True, f"{count} symbols with non-null RSI in gold.stock_summary"
        return False, "gold.stock_summary exists but RSI is null — spark_gold_summary may not have run"
    except Exception as e:
        return False, f"Query failed: {e}"


def check_market_pulse():
    out = _psql("SELECT COUNT(*) FROM gold.v_market_pulse;")
    try:
        count = int(out.split()[0])
        return (
            count > 0,
            f"{count} rows in gold.v_market_pulse"
            if count > 0 else "gold.v_market_pulse is empty — create_views task must run",
        )
    except Exception as e:
        return False, str(e)


def check_latest_prices():
    out = _psql("SELECT COUNT(*) FROM gold.v_latest_prices;")
    try:
        count = int(out.split()[0])
        return (
            count > 0,
            f"{count} symbols in gold.v_latest_prices"
            if count > 0 else "gold.v_latest_prices is empty",
        )
    except Exception as e:
        return False, str(e)


def main():
    print("\n" + "=" * 60)
    print(" FINSCOPE Sprint 3 Verification")
    print("=" * 60 + "\n")

    checks = [
        ("Silver Delta Lake",      check_silver_delta),
        ("gold.stock_summary RSI", check_gold_summary),
        ("gold.v_market_pulse",    check_market_pulse),
        ("gold.v_latest_prices",   check_latest_prices),
    ]
    results = []
    for name, fn in checks:
        start = time.time()
        try:
            passed, msg = fn()
        except Exception as e:
            passed, msg = False, str(e)
        elapsed = time.time() - start
        status = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"  [{status}] {name:30} ({elapsed:.2f}s) — {msg}")
        results.append(passed)

    print()
    if all(results):
        print(f"{GREEN}✓ Sprint 3 fully verified — ready for Sprint 4 DAG trigger{RESET}")
        return 0
    else:
        failed = sum(1 for r in results if not r)
        print(f"{RED}✗ {failed} check(s) failed — fix above before triggering full DAG{RESET}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
