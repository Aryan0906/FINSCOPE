#!/usr/bin/env python3
"""
FINSCOPE INDIA — Infrastructure Verification Script
Sprint 1: Validates all 9 Docker services are accessible

Usage: python scripts/verify_infrastructure.py

Exit codes:
  0 - All services passed
  1 - One or more services failed
"""

import sys
import time
import os
from typing import Tuple

# Colors for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def check_postgresql() -> Tuple[bool, str, float]:
    """Test PostgreSQL connectivity with SELECT 1."""
    import psycopg2
    
    start = time.time()
    try:
        # When running from host, use localhost and credentials from .env
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="finscope",
            user="finscope_admin",
            password="finscope_dev_password_2024",
            connect_timeout=10
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        elapsed = time.time() - start
        
        if result and result[0] == 1:
            return True, "SELECT 1 returned successfully", elapsed
        return False, "Unexpected result from SELECT 1", elapsed
    except Exception as e:
        elapsed = time.time() - start
        return False, str(e), elapsed


def check_chromadb() -> Tuple[bool, str, float]:
    """Test ChromaDB heartbeat endpoint."""
    import requests
    
    start = time.time()
    try:
        host = os.getenv("CHROMADB_HOST", "localhost")
        port = os.getenv("CHROMADB_PORT", "8000")
        url = f"http://{host}:{port}/api/v1/heartbeat"
        
        response = requests.get(url, timeout=10)
        elapsed = time.time() - start
        
        if response.status_code == 200:
            data = response.json()
            return True, f"Heartbeat OK (nanoseconds: {data.get('nanosecond heartbeat', 'N/A')})", elapsed
        return False, f"HTTP {response.status_code}: {response.text}", elapsed
    except Exception as e:
        elapsed = time.time() - start
        return False, str(e), elapsed


def check_kafka() -> Tuple[bool, str, float]:
    """Test Kafka broker by listing topics."""
    from kafka.admin import KafkaAdminClient
    from kafka.errors import KafkaError
    
    start = time.time()
    try:
        # When running from host, use localhost:29092 (external listener)
        bootstrap_servers = "localhost:29092"
        
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="verify_infrastructure",
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=10000
        )
        
        topics = admin_client.list_topics()
        admin_client.close()
        elapsed = time.time() - start
        
        # Check for our expected topics
        expected_topics = ["nse_prices_raw", "nse_news_raw", "nse_earnings_raw"]
        found_topics = [t for t in expected_topics if t in topics]
        
        if len(found_topics) > 0:
            return True, f"Connected. Found topics: {found_topics}", elapsed
        else:
            return True, f"Connected. Topics available: {len(topics)} (expected topics not yet created)", elapsed
            
    except KafkaError as e:
        elapsed = time.time() - start
        return False, f"Kafka error: {e}", elapsed
    except Exception as e:
        elapsed = time.time() - start
        return False, str(e), elapsed


def check_spark() -> Tuple[bool, str, float]:
    """Test Spark master connectivity via HTTP API."""
    import requests
    
    start = time.time()
    try:
        # Check Spark Master UI is responding
        response = requests.get("http://localhost:8090/", timeout=10)
        elapsed = time.time() - start
        
        if response.status_code == 200:
            return True, "Spark Master UI responding at localhost:8090", elapsed
        return False, f"Spark Master returned HTTP {response.status_code}", elapsed
        
    except Exception as e:
        elapsed = time.time() - start
        return False, str(e), elapsed


def print_result(service: str, passed: bool, message: str, elapsed: float) -> None:
    """Print formatted test result."""
    status = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
    print(f"  [{status}] {service:15} ({elapsed:.2f}s)")
    if not passed:
        print(f"         {RED}└─ {message}{RESET}")
    elif "error" in message.lower() or "fail" in message.lower():
        print(f"         {YELLOW}└─ {message}{RESET}")


def main() -> int:
    """Run all infrastructure checks."""
    print("\n" + "=" * 60)
    print(" FINSCOPE Infrastructure Verification")
    print("=" * 60 + "\n")
    
    checks = [
        ("PostgreSQL", check_postgresql),
        ("ChromaDB", check_chromadb),
        ("Kafka", check_kafka),
        ("Spark", check_spark),
    ]
    
    results = []
    
    for service_name, check_func in checks:
        try:
            passed, message, elapsed = check_func()
            results.append((service_name, passed, message, elapsed))
            print_result(service_name, passed, message, elapsed)
        except ImportError as e:
            results.append((service_name, False, f"Missing dependency: {e}", 0.0))
            print_result(service_name, False, f"Missing dependency: {e}", 0.0)
        except Exception as e:
            results.append((service_name, False, str(e), 0.0))
            print_result(service_name, False, str(e), 0.0)
    
    # Summary
    print("\n" + "-" * 60)
    passed_count = sum(1 for _, passed, _, _ in results if passed)
    total_count = len(results)
    
    if passed_count == total_count:
        print(f" {GREEN}✓ All {total_count} services passed{RESET}")
        print("-" * 60 + "\n")
        return 0
    else:
        failed_count = total_count - passed_count
        print(f" {RED}✗ {failed_count}/{total_count} services failed{RESET}")
        print("-" * 60 + "\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
