"""Typesense benchmark runner."""

import os
import time
import json
import csv
from typing import Any

from . import BaseRunner, BenchmarkResult, EngineConfig, register_engine


def _parse_row(row: dict) -> dict:
    """Parse CSV row - convert numeric strings to numbers."""
    result = {}
    for key, val in row.items():
        if key == "id":
            # Keep id as string for Typesense (it's declared as string type)
            result[key] = str(val) if val else val
        elif isinstance(val, str):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, (dict, list)):
                    result[key] = parsed
                else:
                    result[key] = _coerce_number(val)
            except (json.JSONDecodeError, TypeError):
                result[key] = _coerce_number(val)
        else:
            result[key] = val
    return result


def _coerce_number(val: str):
    if not isinstance(val, str):
        return val
    try:
        return int(val)
    except ValueError:
        pass
    try:
        return float(val)
    except ValueError:
        pass
    return val


@register_engine("typesense")
class TypesenseRunner(BaseRunner):
    """Benchmark runner for Typesense."""

    name = "typesense"
    supports_aggregate = True
    supports_range_query = True
    supports_wildcard = True

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self.base_url = config.connection_string or "http://localhost:8108"
        self.index_name = config.options.get("index_name", "benchmark")
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from typesense import Client
                self._client = Client({
                    'nodes': [{'host': 'localhost', 'port': '8108', 'protocol': 'http'}],
                    'connection_timeout_seconds': 30,
                })
            except ImportError:
                raise ImportError("typesense-py not installed. Run: pip install typesense")
            except Exception as e:
                raise RuntimeError(f"Failed to connect to Typesense: {e}")
        return self._client

    def _request(self, method: str, path: str, body=None) -> tuple:
        """Make HTTP request to Typesense. Returns (status, response_dict)."""
        import urllib.request
        import urllib.error

        url = f"{self.base_url}{path}"
        if isinstance(body, str):
            data = body.encode()
        elif body is not None:
            data = json.dumps(body).encode()
        else:
            data = None

        headers = {"Content-Type": "application/x-ndjson" if isinstance(body, str) else "application/json", "x-typesense-api-key": "xyz"}
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                status = resp.status
                # Read all data - handle large responses (bulk import)
                raw_parts = []
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    raw_parts.append(chunk)
                raw = b"".join(raw_parts).decode()
                # Handle NDJSON responses (bulk import returns one JSON per line)
                try:
                    resp_body = json.loads(raw)
                except json.JSONDecodeError:
                    # Try NDJSON format
                    lines = [json.loads(l) for l in raw.strip().split("\n") if l.strip()]
                    resp_body = {"documents": lines}
                return status, resp_body
        except urllib.error.HTTPError as e:
            try:
                err_body = json.loads(e.read().decode())
            except:
                err_body = {"error": str(e)}
            return e.code, err_body
        except Exception as e:
            return 0, {"error": str(e)}

    def build_index(self, data_path: str, workers: int = 1) -> BenchmarkResult:
        """Build index from CSV data via Typesense bulk API."""
        index_name = self.index_name

        # Delete existing index first
        self._request("DELETE", f"/collections/{index_name}")

        # Create index with proper schema including facet fields
        create_body = {
            "name": index_name,
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "title", "type": "string"},
                {"name": "content", "type": "string"},
                {"name": "tags", "type": "string[]", "facet": True},
                {"name": "views", "type": "int64"},
                {"name": "published_at", "type": "string"},
                {"name": "author", "type": "string", "facet": True},
                {"name": ".*", "type": "auto"}
            ],
            "default_sorting_field": "views"
        }
        _, resp = self._request("POST", "/collections", create_body)
        if resp.get("error"):
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(data_path),
                operation="build_index",
                rows=0,
                duration_ms=0,
                ops_per_sec=0,
                error=f"Failed to create collection: {resp.get('error')}"
            )

        # Bulk index CSV data
        start_time = time.perf_counter()
        rows_indexed = 0
        bulk_size = 1000
        docs_batch = []

        with open(data_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                flat_row = _parse_row(row)
                docs_batch.append(flat_row)
                rows_indexed += 1

                if len(docs_batch) >= bulk_size:
                    body = "\n".join(json.dumps(d) for d in docs_batch)
                    status, resp = self._request("POST", f"/collections/{index_name}/documents/import", body)
                    # Check for import errors - resp may be {"documents": [...]} for NDJSON
                    if isinstance(resp, dict):
                        if resp.get("error"):
                            return BenchmarkResult(...)
                        docs = resp.get("documents", [])
                        # Filter out expected errors (409 = already exists, retry-safe)
                        real_errors = [d for d in docs if isinstance(d, dict) and not d.get("success") and d.get("code") not in (409,)]
                        if real_errors:
                            return BenchmarkResult(
                                engine=self.name,
                                dataset=os.path.basename(data_path),
                                operation="build_index",
                                rows=rows_indexed,
                                duration_ms=(time.perf_counter() - start_time) * 1000,
                                ops_per_sec=0,
                                error=f"Import errors: {real_errors[:3]}"
                            )
                    docs_batch = []

                if rows_indexed % 50000 == 0:
                    print(f"  Typesense: {rows_indexed:,} docs indexed...")

        # Flush remaining docs
        if docs_batch:
            body = "\n".join(json.dumps(d) for d in docs_batch)
            status, resp = self._request("POST", f"/collections/{index_name}/documents/import", body)

        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = rows_indexed / (duration_ms / 1000) if duration_ms > 0 else 0

        # Get index stats
        _, stats_resp = self._request("GET", f"/collections/{index_name}")
        index_size_mb = 0

        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(data_path),
            operation="build_index",
            rows=rows_indexed,
            duration_ms=duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=duration_ms,
            latency_p95_ms=duration_ms,
            latency_p99_ms=duration_ms,
            memory_mb=index_size_mb,
            metadata={
                "index_size_mb": index_size_mb,
            }
        )

    def search(self, query: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run search queries via Typesense API."""
        index_name = self.index_name
        latencies = []
        result_counts = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            status, resp = self._request("POST", "/multi_search", {
                "searches": [{
                    "collection": index_name,
                    "q": query,
                    "query_by": "*",
                    "limit": 250
                }]
            })
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000)

            if status == 200 and resp.get("results"):
                result_counts.append(resp["results"][0].get("found", 0))
            else:
                result_counts.append(0)

        end_time = time.perf_counter()
        total_duration_ms = (end_time - start_time) * 1000
        ops_per_sec = iterations / (total_duration_ms / 1000) if total_duration_ms > 0 else 0

        latencies.sort()
        p50 = latencies[int(len(latencies) * 0.50)]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]

        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(self.config.data_dir),
            operation="search",
            rows=iterations,
            duration_ms=total_duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=p50,
            latency_p95_ms=p95,
            latency_p99_ms=p99,
            metadata={"query": query, "result_count": result_counts[-1] if result_counts else 0}
        )

    def aggregate(self, field: str, query: str = "*", agg_type: str = "terms", **kwargs) -> BenchmarkResult:
        """Run aggregation queries via Typesense API."""
        index_name = self.index_name

        start_time = time.perf_counter()

        # Typesense uses facets for aggregation-like functionality
        if agg_type in ("terms", "cardinality"):
            body = {
                "searches": [{
                    "collection": index_name,
                    "q": query if query != "*" else "*",
                    "query_by": "*",
                    "facet_by": field,
                    "max_facet_values": 100,
                    "per_page": 0
                }]
            }
        else:
            body = {
                "searches": [{
                    "collection": index_name,
                    "q": query if query != "*" else "*",
                    "query_by": "*",
                }]
            }

        status, resp = self._request("POST", "/multi_search", body)
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0

        rows = 0
        if status == 200 and resp.get("results"):
            facet_counts = resp["results"][0].get("facet_counts", [])
            if facet_counts:
                buckets = facet_counts[0].get("counts", [])
                rows = len(buckets)

        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(self.config.data_dir),
            operation="aggregate",
            rows=rows,
            duration_ms=duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=duration_ms,
            latency_p95_ms=duration_ms,
            latency_p99_ms=duration_ms,
            metadata={"field": field, "agg_type": agg_type, "bucket_count": rows}
        )

    def range_query(self, field: str, lo: Any, hi: Any, **kwargs) -> BenchmarkResult:
        """Run range queries via Typesense API."""
        index_name = self.index_name
        query = f"{field}:{lo}..{hi}"

        start_time = time.perf_counter()
        status, resp = self._request("POST", "/multi_search", {
            "searches": [{
                "collection": index_name,
                "q": "*",
                "query_by": "*",
                "filter_by": query,
                "per_page": 250
            }]
        })
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0
        hits = 0
        if status == 200 and resp.get("results"):
            hits = resp["results"][0].get("found", 0)

        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(self.config.data_dir),
            operation="range_query",
            rows=hits,
            duration_ms=duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=duration_ms,
            latency_p95_ms=duration_ms,
            latency_p99_ms=duration_ms,
            metadata={"query": query, "result_count": hits}
        )

    def wildcard_search(self, pattern: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run wildcard search queries via Typesense API."""
        index_name = self.index_name
        latencies = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            self._request("POST", "/multi_search", {
                "searches": [{
                    "collection": index_name,
                    "q": f"*{pattern}*",
                    "query_by": "*",
                    "per_page": 250
                }]
            })
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000)
        end_time = time.perf_counter()

        total_duration_ms = (end_time - start_time) * 1000
        ops_per_sec = iterations / (total_duration_ms / 1000) if total_duration_ms > 0 else 0

        latencies.sort()
        p50 = latencies[int(len(latencies) * 0.50)]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]

        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(self.config.data_dir),
            operation="wildcard_search",
            rows=iterations,
            duration_ms=total_duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=p50,
            latency_p95_ms=p95,
            latency_p99_ms=p99,
            metadata={"pattern": pattern}
        )

    def cleanup(self):
        """Delete the collection."""
        self._request("DELETE", f"/collections/{self.index_name}")
