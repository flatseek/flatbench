"""Flatseek benchmark runner via HTTP API (Docker mode).

Uses flatseek API server running in Docker, similar to ElasticsearchRunner.
All operations (bulk index, search, aggregate) use HTTP API calls.
"""

import os
import sys
import time
import json
import csv
import urllib.request
import urllib.error
from typing import Any

from . import BaseRunner, BenchmarkResult, EngineConfig, register_engine


def _parse_row(row: dict) -> dict:
    """Parse CSV row - convert numeric strings to numbers."""
    result = {}
    for key, val in row.items():
        if isinstance(val, str):
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


@register_engine("flatseek_api")
class FlatseekApiRunner(BaseRunner):
    """Benchmark runner for Flatseek via HTTP API (Docker)."""

    name = "flatseek_api"
    supports_aggregate = True
    supports_range_query = True
    supports_wildcard = True

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self.base_url = config.connection_string or "http://localhost:8000"
        self.index_name = config.options.get("index_name", "benchmark")
        self._headers = {"Content-Type": "application/json"}

    def _request(self, method: str, path: str, body=None) -> tuple:
        """Make HTTP request to flatseek API. Returns (status, response_dict)."""
        url = f"{self.base_url}{path}"
        if isinstance(body, str):
            data = body.encode()
        elif body is not None:
            data = json.dumps(body).encode()
        else:
            data = None

        req = urllib.request.Request(url, data=data, headers=self._headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                status = resp.status
                try:
                    resp_body = json.loads(resp.read().decode())
                except:
                    resp_body = {}
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
        """Build index from CSV data via flatseek bulk API."""
        index_name = self.index_name

        # Delete existing index first
        self._request("DELETE", f"/{index_name}")

        # Create index
        _, resp = self._request("PUT", f"/{index_name}", {})
        if resp.get("error"):
            print(f"  Create index warning: {resp.get('error')}")

        # Bulk index CSV data - flatseek API expects JSON array of documents
        start_time = time.perf_counter()
        rows_indexed = 0
        bulk_size = 1000
        docs_batch = []
        headers = None

        with open(data_path, "r") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            for row in reader:
                flat_row = _parse_row(row)
                docs_batch.append(flat_row)
                rows_indexed += 1

                if len(docs_batch) >= bulk_size:
                    status, resp = self._request("POST", f"/{index_name}/_bulk", docs_batch)
                    if resp.get("error"):
                        return BenchmarkResult(
                            engine=self.name, dataset=os.path.basename(data_path),
                            operation="build_index", rows=rows_indexed,
                            duration_ms=(time.perf_counter() - start_time) * 1000,
                            ops_per_sec=0,
                            error=f"Bulk index error: {resp.get('error')}"
                        )
                    docs_batch = []

                if rows_indexed % 50000 == 0:
                    print(f"  flatseek: {rows_indexed:,} rows indexed...")

        # Flush remaining docs and wait for completion
        if docs_batch:
            status, resp = self._request("POST", f"/{index_name}/_bulk", docs_batch)
            if resp.get("error"):
                return BenchmarkResult(
                    engine=self.name, dataset=os.path.basename(data_path),
                    operation="build_index", rows=rows_indexed,
                    duration_ms=(time.perf_counter() - start_time) * 1000,
                    ops_per_sec=0,
                    error=f"Bulk index error: {resp.get('error')}"
                )

        # Flush to ensure all data is written to disk before querying
        print(f"  flatseek: flushing...")
        _, flush_resp = self._request("POST", f"/{index_name}/_flush?wait=true")
        if flush_resp.get("detail") and "error" not in str(flush_resp):
            pass  # flush completed

        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = rows_indexed / (duration_ms / 1000) if duration_ms > 0 else 0

        # Get index stats
        _, stats_resp = self._request("GET", f"/{index_name}/_stats")
        index_size_mb = stats_resp.get("index_size_mb", 0)

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
                "columns_count": len(headers) if headers else 0,
            }
        )

    def search(self, query: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run search queries via flatseek API."""
        index_name = self.index_name
        latencies = []
        result_counts = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            status, resp = self._request("POST", f"/{index_name}/_search", {"q": query, "size": 1000})
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000)

            if status == 200:
                result_counts.append(resp.get("hits", {}).get("total", 0))
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
        """Run aggregation queries via flatseek API."""
        index_name = self.index_name

        aggs = {agg_type: {"field": field, "size": 100}}
        body = {"q": query if query != "*" else "*", "aggs": aggs}

        start_time = time.perf_counter()
        status, resp = self._request("POST", f"/{index_name}/_aggregate", body)
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0

        agg_result = resp.get("aggregations", {})
        agg_value = agg_result.get(field, agg_result.get(agg_type, {}))

        if agg_type == "terms":
            buckets = agg_value.get("buckets", [])
            rows = len(buckets)
        else:
            rows = 1

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
        """Run range queries via flatseek API."""
        index_name = self.index_name
        query = f"{field}:[{lo} TO {hi}]"

        start_time = time.perf_counter()
        status, resp = self._request("POST", f"/{index_name}/_search", {"q": query, "size": 0})
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0
        hits = resp.get("hits", {}).get("total", 0)

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
        """Run wildcard search queries via flatseek API."""
        index_name = self.index_name
        latencies = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            self._request("POST", f"/{index_name}/_search", {"q": f"*{pattern}*", "size": 1000})
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
        """Delete the index."""
        self._request("DELETE", f"/{self.index_name}")
