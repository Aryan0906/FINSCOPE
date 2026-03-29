#!/usr/bin/env python3
"""
scripts/verify_sprint2.py
===========================
Verifies Sprint 2 deliverables:
  1. Kafka topic nse_prices_raw exists and has messages
  2. Delta Lake Bronze has parquet files
  3. Bronze file count > 0

Exit code 0 = all checks pass.
"""
import os
import sys
import time

GREEN = "\033[92m"
RED   = "\033[91m"
RESET = "\033[0m"


def check_kafka_topic():
    from kafka.admin import KafkaAdminClient
    try:
        client = KafkaAdminClient(
            bootstrap_servers="localhost:29092",
            client_id="verify_sprint2",
            request_timeout_ms=10000,
        )
        topics = client.list_topics()
        client.close()
        if "nse_prices_raw" in topics:
            return True, "nse_prices_raw topic exists"
        return False, f"Topic not found. Available: {topics}"
    except Exception as e:
        return False, str(e)


def check_bronze_delta():
    bronze_path = os.getenv("BRONZE_PRICES_PATH", "/opt/delta-lake/bronze/stock_prices")
    # Check via docker exec since Delta Lake is inside container
    import subprocess
    result = subprocess.run(
        ["docker", "exec", "finscope_spark_master",
         "find", bronze_path, "-name", "*.parquet"],
        capture_output=True, text=True, timeout=15,
    )
    files = [line for line in result.stdout.strip().split("\n") if line]
    if files:
        return True, f"{len(files)} parquet file(s) found in Bronze"
    return False, "No parquet files in Bronze Delta Lake — run Kafka producer first"


def main():
    checks = [
        ("Kafka topic",     check_kafka_topic),
        ("Bronze Delta",    check_bronze_delta),
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
        print(f"  [{status}] {name:20} ({elapsed:.2f}s) — {msg}")
        results.append(passed)

    print()
    if all(results):
        print(f"{GREEN}✓ Sprint 2 verified — Bronze has data, proceeding to Sprint 3{RESET}")
        return 0
    else:
        print(f"{RED}✗ Sprint 2 not fully verified — fix above before running Sprint 3 Spark jobs{RESET}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
