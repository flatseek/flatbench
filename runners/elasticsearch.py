"""Elasticsearch benchmark runner."""

import os
import sys
import time
import json
import urllib.request
import urllib.error
from typing import Any

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

def _es_daemon_rss_mb() -> float:
    """Get Elasticsearch daemon RSS memory in MB. Returns 0 if not found."""
    if not PSUTIL_AVAILABLE:
        return 0.0
    es_procs = []
    for proc in psutil.process_iter(["name", "memory_info"]):
        try:
            name = proc.info.get("name", "")
            # ES daemon is java named "java" with cmdline containing "elasticsearch"
            if name == "java" or name == "java32" or name == "javac":
                cmdline = proc.cmdline() if hasattr(proc, "cmdline") else []
                cmdline_str = " ".join(cmdline) if cmdline else ""
                if "elasticsearch" in cmdline_str.lower():
                    es_procs.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    # Sum RSS of all ES processes
    total_mb = 0.0
    for proc in es_procs:
        try:
            mem = proc.memory_info()
            total_mb += mem.rss / 1024 / 1024
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return total_mb


from . import BaseRunner, BenchmarkResult, EngineConfig, register_engine


def _parse_row(row: dict) -> dict:
    """Parse CSV row JSON strings back into native JSON objects for ES.

    Nested fields stored as JSON strings in CSV are parsed back to objects/arrays.
    Tags (array of strings) are also parsed from JSON string to actual array.
    Numeric-looking strings (for fields like views, id) are converted to numbers
    so ES stores them as numeric types for correct range query behavior.
    """
    result = {}
    for key, val in row.items():
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                # Only replace with parsed object/array if it's actually structured
                if isinstance(parsed, (dict, list)):
                    result[key] = parsed
                else:
                    # Try to convert numeric-looking strings to numbers
                    result[key] = _coerce_number(val)
            except (json.JSONDecodeError, TypeError):
                # Try to convert numeric-looking strings to numbers
                result[key] = _coerce_number(val)
        else:
            result[key] = val
    return result


def _coerce_number(val: str):
    """Convert a string to int/float if it looks like a number."""
    if not isinstance(val, str):
        return val
    # Try int first
    try:
        return int(val)
    except ValueError:
        pass
    # Try float
    try:
        return float(val)
    except ValueError:
        pass
    return val


@register_engine("elasticsearch")
class ElasticsearchRunner(BaseRunner):
    """Benchmark runner for Elasticsearch."""

    name = "elasticsearch"
    supports_aggregate = True
    supports_range_query = True
    supports_wildcard = True

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self.base_url = config.connection_string or "http://localhost:9200"
        self.index_name = config.options.get("index_name", "benchmark")
        self._headers = {"Content-Type": "application/json"}

    def _request(self, method: str, path: str, body=None) -> tuple:
        """Make HTTP request to Elasticsearch. Returns (status, response_dict).

        Args:
            body: Can be dict (JSON-encoded) or string (sent as-is for bulk requests)
        """
        url = f"{self.base_url}{path}"
        if isinstance(body, str):
            data = body.encode()
        elif body is not None:
            data = json.dumps(body).encode()
        else:
            data = None

        req = urllib.request.Request(url, data=data, headers=self._headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
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

    def build_index(self, data_path: str) -> BenchmarkResult:
        """Build index from CSV data via Elasticsearch bulk API."""
        import csv

        index_name = self.index_name

        # Delete existing index
        self._request("DELETE", f"/{index_name}")

        # Measure ES daemon RSS before indexing (baseline)
        es_rss_before_mb = _es_daemon_rss_mb()

        # Create index with dynamic mapping — ES auto-detects nested JSON structures
        settings = {"number_of_shards": 1, "number_of_replicas": 0}
        mappings = {"dynamic": "true", "properties": {}}
        _, resp = self._request("PUT", f"/{index_name}", {"settings": settings, "mappings": mappings})
        if _ != 200 and _ != 201:
            return BenchmarkResult(
                engine=self.name, dataset=os.path.basename(data_path),
                operation="build_index", rows=0, duration_ms=0, ops_per_sec=0,
                error=f"Failed to create index: {resp}"
            )

        # Bulk index CSV data
        start_time = time.perf_counter()
        rows_indexed = 0
        bulk_size = 1000
        bulk_body = []
        headers = None

        with open(data_path, "r") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            for row in reader:
                # Parse CSV JSON strings back to native JSON objects (no flattening)
                flat_row = _parse_row(row)
                action = {"index": {"_index": index_name}}
                bulk_body.append(json.dumps(action))
                bulk_body.append(json.dumps(flat_row))
                rows_indexed += 1

                if len(bulk_body) >= bulk_size * 2:
                    body = "\n".join(bulk_body) + "\n"
                    status, resp = self._request("POST", "/_bulk", body)
                    if status not in (200, 201):
                        return BenchmarkResult(
                            engine=self.name, dataset=os.path.basename(data_path),
                            operation="build_index", rows=rows_indexed,
                            duration_ms=(time.perf_counter() - start_time) * 1000,
                            ops_per_sec=0,
                            error=f"Bulk index error: {resp}"
                        )
                    bulk_body = []

                if rows_indexed % 50000 == 0:
                    print(f"  ES: {rows_indexed:,} rows indexed...")

        # Flush remaining
        if bulk_body:
            body = "\n".join(bulk_body) + "\n"
            self._request("POST", "/_bulk", body)

        # Refresh index
        self._request("POST", f"/{index_name}/_refresh")

        # Measure ES daemon RSS after indexing
        es_rss_after_mb = _es_daemon_rss_mb()
        es_rss_delta_mb = max(0.0, es_rss_after_mb - es_rss_before_mb)

        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = rows_indexed / (duration_ms / 1000) if duration_ms > 0 else 0

        # Get index stats
        _, stats_resp = self._request("GET", f"/{index_name}/_stats")
        index_size_mb = stats_resp.get("_all", {}).get("primaries", {}).get("store", {}).get("size_in_bytes", 0) / 1024 / 1024

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
                "columns_count": len(headers),
                "es_rss_before_mb": es_rss_before_mb,
                "es_rss_after_mb": es_rss_after_mb,
                "es_rss_delta_mb": es_rss_delta_mb,
            }
        )

    def search(self, query: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run search queries."""
        index_name = self.index_name
        latencies = []
        result_counts = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()

            # Convert flatseek query to ES query
            es_query = self._flatseek_to_es(query)

            status, resp = self._request("POST", f"/{index_name}/_search", es_query)
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000)

            if status == 200:
                result_counts.append(resp.get("hits", {}).get("total", {}).get("value", 0))
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

    def _flatseek_to_es(self, query: str) -> dict:
        """Convert flatseek-style query to Elasticsearch query DSL."""
        query = query.strip()

        # Match all
        if query == "*":
            return {"query": {"match_all": {}}, "size": 1000}

        # Exact term (field:value)
        if ":" in query and not query.startswith("["):
            parts = query.split(":", 1)
            field = parts[0]
            value = parts[1].strip()

            # Wildcard in value
            if "*" in value or "%" in value:
                es_value = value.replace("%", "*")
                return {"query": {"wildcard": {field: {"value": es_value}}}, "size": 1000}

            # Range query [lo TO hi]
            if value.startswith("[") and " TO " in value:
                # Parse range
                inner = value[1:-1]
                if " TO " in inner:
                    lo_hi = inner.split(" TO ", 1)
                    return {
                        "query": {"range": {field: {"gte": lo_hi[0], "lte": lo_hi[1]}}},
                        "size": 1000
                    }

            # Regular exact match
            key = f"{field}.keyword" if "." in field else field
            return {"query": {"term": {key: value}}, "size": 1000}

        # Bare term - search all text fields
        return {"query": {"query_string": {"query": query}}, "size": 1000}

    def aggregate(self, field: str, query: str = "*", agg_type: str = "terms", **kwargs) -> BenchmarkResult:
        """Run aggregation queries."""
        index_name = self.index_name

        # Use .keyword sub-field for terms/cardinality aggregations on text fields
        # (ES stores tags/author as text, so .keyword is required for terms aggs)
        # For date fields (published_at), .keyword doesn't exist - use date_histogram instead
        DATE_FIELD = "published_at"
        if agg_type == "terms" and field == DATE_FIELD:
            # Use date_histogram for year extraction from date fields
            es_body = {
                "size": 0,
                "aggs": {
                    "my_agg": {
                        "date_histogram": {
                            "field": field,
                            "calendar_interval": "year",
                            "format": "yyyy"
                        }
                    }
                }
            }
            if query != "*":
                es_body["query"] = self._flatseek_to_es(query)["query"]
            start_time = time.perf_counter()
            status, resp = self._request("POST", f"/{index_name}/_search", es_body)
            end_time = time.perf_counter()
            duration_ms = (end_time - start_time) * 1000
            buckets = resp.get("aggregations", {}).get("my_agg", {}).get("buckets", [])
            rows = len(buckets)
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(self.config.data_dir),
                operation="aggregate",
                rows=rows,
                duration_ms=duration_ms,
                ops_per_sec=1000 / duration_ms if duration_ms > 0 else 0,
                latency_p50_ms=duration_ms,
                latency_p95_ms=duration_ms,
                latency_p99_ms=duration_ms,
                metadata={"field": field, "agg_type": agg_type, "bucket_count": rows}
            )
        agg_field = f"{field}.keyword" if agg_type in ("terms", "cardinality") else field

        if agg_type == "terms":
            es_body = {
                "size": 0,
                "aggs": {
                    "my_agg": {
                        "terms": {"field": agg_field, "size": 100}
                    }
                }
            }
        elif agg_type in ("stats", "min", "max", "sum", "avg"):
            es_body = {
                "size": 0,
                "aggs": {
                    "my_agg": {
                        agg_type: {"field": field}
                    }
                }
            }
        elif agg_type == "cardinality":
            es_body = {
                "size": 0,
                "aggs": {
                    "my_agg": {
                        "cardinality": {"field": agg_field}
                    }
                }
            }
        else:
            es_body = {
                "size": 0,
                "aggs": {
                    "my_agg": {
                        "terms": {"field": agg_field, "size": 100}
                    }
                }
            }

        if query != "*":
            es_body["query"] = self._flatseek_to_es(query)["query"]

        start_time = time.perf_counter()
        status, resp = self._request("POST", f"/{index_name}/_search", es_body)
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0

        agg_result = resp.get("aggregations", {}).get("my_agg", {})

        if agg_type == "terms":
            buckets = agg_result.get("buckets", [])
            rows = len(buckets)
        elif agg_type in ("stats", "min", "max", "sum", "avg", "cardinality"):
            rows = 1
        else:
            rows = 0

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
        """Run range queries."""
        index_name = self.index_name

        es_query = {
            "query": {"range": {field: {"gte": lo, "lte": hi}}},
            "size": 1000,
            "track_total_hits": True
        }

        start_time = time.perf_counter()
        status, resp = self._request("POST", f"/{index_name}/_search", es_query)
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0

        hits = resp.get("hits", {}).get("total", {}).get("value", 0)

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
            metadata={"field": field, "lo": lo, "hi": hi, "result_count": hits}
        )

    def wildcard_search(self, pattern: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run wildcard search queries."""
        index_name = self.index_name
        latencies = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            es_query = {
                "query": {"query_string": {"query": f"*{pattern}*"}},
                "size": 1000
            }
            self._request("POST", f"/{index_name}/_search", es_query)
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