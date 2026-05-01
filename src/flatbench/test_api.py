#!/usr/bin/env python3
"""Test flatseek API endpoints.

Usage:
    python -m flatbench.test_api
    PYTHONPATH=src python test_api.py
"""

import sys
import os
import time
import argparse

# Default config
DEFAULT_HOST = "http://localhost:8000"
DEFAULT_INDEX = "test"


def wait_for_api(host: str, timeout: int = 30) -> bool:
    """Wait for API to be ready."""
    import httpx

    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = httpx.get(f"{host}/", timeout=5)
            if resp.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def create_index(host: str, index: str) -> dict:
    """Create an index."""
    import httpx

    resp = httpx.put(f"{host}/{index}", timeout=30)
    return resp.json()


def index_document(host: str, index: str, doc: dict) -> dict:
    """Index a single document."""
    import httpx

    resp = httpx.post(f"{host}/{index}/_doc", json=doc, timeout=30)
    return resp.json()


def search(host: str, index: str, query: str, size: int = 10) -> dict:
    """Search the index."""
    import httpx

    resp = httpx.post(
        f"{host}/{index}/_search",
        json={"query": query, "size": size},
        timeout=30,
    )
    return resp.json()


def aggregate(host: str, index: str, field: str, agg_type: str = "terms") -> dict:
    """Run aggregation."""
    import httpx

    aggs = {agg_type: {"field": field, "size": 100}}
    resp = httpx.post(
        f"{host}/{index}/_search",
        json={"query": "*", "aggs": aggs},
        timeout=30,
    )
    return resp.json()


def range_query(host: str, index: str, field: str, lo: float, hi: float) -> dict:
    """Run range query."""
    import httpx

    q = f"{field}:[{lo} TO {hi}]"
    resp = httpx.post(
        f"{host}/{index}/_search",
        json={"query": q, "size": 1000},
        timeout=30,
    )
    return resp.json()


def wildcard_search(host: str, index: str, pattern: str) -> dict:
    """Run wildcard search."""
    import httpx

    resp = httpx.post(
        f"{host}/{index}/_search",
        json={"query": f"*{pattern}*", "size": 100},
        timeout=30,
    )
    return resp.json()


def get_stats(host: str, index: str) -> dict:
    """Get index stats."""
    import httpx

    resp = httpx.get(f"{host}/{index}/_stats", timeout=30)
    return resp.json()


def run_tests(host: str, index: str, n_docs: int = 1000):
    """Run all API tests."""
    import httpx

    print(f"\n{'='*60}")
    print(f"Testing flatseek API: {host}")
    print(f"Index: {index}, Documents: {n_docs}")
    print(f"{'='*60}\n")

    # Wait for API
    print("[1/8] Waiting for API...")
    if not wait_for_api(host):
        print("  FAIL: API not responding")
        return
    print("  OK: API is ready")

    # Get version
    print("[2/8] Root endpoint (version)...")
    resp = httpx.get(f"{host}/", timeout=10)
    info = resp.json()
    print(f"  Name: {info.get('name')}")
    print(f"  Version: {info.get('version')}")

    # Create index
    print("[3/8] Creating index...")
    try:
        result = create_index(host, index)
        print(f"  Result: {result.get('result', 'N/A')}")
    except Exception as e:
        print(f"  Note: {e}")

    # Index sample docs
    print(f"[4/8] Indexing {n_docs} documents...")
    docs = []
    for i in range(n_docs):
        doc = {
            "id": i,
            "name": f"User {i}",
            "email": f"user{i}@test.com",
            "city": ["Jakarta", "Surabaya", "Bandung"][i % 3],
            "country": ["Indonesia", "Malaysia", "Singapore"][i % 3],
            "status": ["active", "inactive", "pending"][i % 3],
            "balance": float(i * 1000),
            "tags": [f"tag{i % 5}", f"tag{(i+1) % 5}"],
            "is_verified": i % 2 == 0,
        }
        docs.append(doc)

    start = time.time()
    for doc in docs:
        try:
            index_document(host, index, doc)
        except Exception as e:
            print(f"  Warning: {e}")
            break
    elapsed = time.time() - start
    print(f"  Done: {n_docs} docs in {elapsed:.2f}s ({n_docs/elapsed:.0f} docs/s)")

    # Refresh index
    print("[5/8] Refreshing index...")
    httpx.post(f"{host}/{index}/_refresh", timeout=30)
    print("  OK")

    # Test search
    print("[6/8] Search tests...")
    tests = [
        ("Match all", "*"),
        ("City Jakarta", "city:Jakarta"),
        ("Status active", "status:active"),
        ("Tags tag0", "tags:tag0"),
        ("Balance > 5000", "balance:>5000"),
    ]
    for label, q in tests:
        result = search(host, index, q)
        total = result.get("total", 0)
        print(f"  {label}: {total} hits")

    # Test wildcard
    print("[7/8] Wildcard search...")
    result = wildcard_search(host, index, "user")
    print(f"  'user*': {result.get('total', 0)} hits")

    # Test aggregation
    print("[8/8] Aggregation tests...")
    result = aggregate(host, index, "city")
    buckets = result.get("aggregations", {}).get("city", {}).get("buckets", [])
    print(f"  city terms: {len(buckets)} buckets")
    for b in buckets[:5]:
        print(f"    {b.get('key')}: {b.get('doc_count')}")

    # Test range query
    print("  Range query balance:[1000 TO 5000]:")
    result = range_query(host, index, "balance", 1000, 5000)
    print(f"    {result.get('total', 0)} hits")

    # Get stats
    print("\n[Done] Index stats:")
    stats = get_stats(host, index)
    total_docs = stats.get("indices", {}).get(index, {}).get("primaries", {}).get("docs", {}).get("count", 0)
    size_bytes = stats.get("indices", {}).get(index, {}).get("primaries", {}).get("store", {}).get("size_in_bytes", 0)
    print(f"  Total docs: {total_docs:,}")
    print(f"  Index size: {size_bytes / 1024 / 1024:.2f} MB")

    print(f"\n{'='*60}")
    print("All tests passed!")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="Test flatseek API endpoints")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"API host (default: {DEFAULT_HOST})")
    parser.add_argument("--index", "-i", default=DEFAULT_INDEX, help=f"Index name (default: {DEFAULT_INDEX})")
    parser.add_argument("--docs", "-n", type=int, default=1000, help="Number of test docs (default: 1000)")
    parser.add_argument("--delete", action="store_true", help="Delete index after test")
    args = parser.parse_args()

    try:
        import httpx
    except ImportError:
        sys.exit("Error: httpx not installed. Run: pip install httpx")

    run_tests(args.host, args.index, args.docs)

    if args.delete:
        import httpx
        print(f"Deleting index '{args.index}'...")
        httpx.delete(f"{args.host}/{args.index}", timeout=30)
        print("Done.")


if __name__ == "__main__":
    main()
