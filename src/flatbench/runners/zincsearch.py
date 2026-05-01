"""ZincSearch benchmark runner."""

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
    # Convert published_at to proper ISO format for ZincSearch
    if "published_at" in result and result["published_at"] is not None:
        result["published_at"] = _coerce_date(str(result["published_at"]))
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


def _coerce_date(val: str):
    """Convert date string to ZincSearch-compatible ISO format."""
    if not isinstance(val, str):
        return val
    # Already ISO format
    if "T" in val or "Z" in val:
        return val
    # Simple date YYYY-MM-DD -> YYYY-MM-DDT00:00:00Z
    if len(val) == 10 and val[4] == "-" and val[7] == "-":
        return f"{val}T00:00:00Z"
    return val


@register_engine("zincsearch")
class ZincSearchRunner(BaseRunner):
    """Benchmark runner for ZincSearch."""

    name = "zincsearch"
    supports_aggregate = True
    supports_range_query = True
    supports_wildcard = True

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self.base_url = config.connection_string or "http://localhost:4080"
        self.index_name = config.options.get("index_name", "benchmark")
        self.user = config.options.get("user", "admin")
        self.password = config.options.get("password", "Complexpass#123")

    def _request(self, method: str, path: str, body=None) -> tuple:
        """Make HTTP request to ZincSearch. Returns (status, response_dict)."""
        import urllib.request
        import urllib.error
        import base64

        url = f"{self.base_url}{path}"
        if isinstance(body, str):
            data = body.encode()
        elif body is not None:
            data = json.dumps(body).encode()
        else:
            data = None

        headers = {"Content-Type": "application/json"}
        credentials = f"{self.user}:{self.password}".encode()
        headers["Authorization"] = "Basic " + base64.b64encode(credentials).decode()

        req = urllib.request.Request(url, data=data, headers=headers, method=method)
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

    def _request_ndjson(self, method: str, path: str, body_lines: list) -> tuple:
        """Make HTTP request with NDJSON body. Returns (status, response_dict)."""
        import urllib.request
        import urllib.error
        import base64

        url = f"{self.base_url}{path}"
        ndjson_body = "\n".join(body_lines).encode()
        headers = {"Content-Type": "application/x-ndjson"}
        credentials = f"{self.user}:{self.password}".encode()
        headers["Authorization"] = "Basic " + base64.b64encode(credentials).decode()

        req = urllib.request.Request(url, data=ndjson_body, headers=headers, method=method)
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

    def _index_exists(self, index_name: str) -> bool:
        """Check if an index already exists."""
        status, _ = self._request("HEAD", f"/es/{index_name}")
        return status == 200

    def _get_unique_index_name(self, base_name: str) -> str:
        """Get a unique index name, appending suffix if base name exists."""
        if not self._index_exists(base_name):
            return base_name
        # Try suffix with timestamp to avoid collision
        import datetime
        suffix = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        candidate = f"{base_name}_{suffix}"
        if not self._index_exists(candidate):
            return candidate
        # Fallback: use random suffix
        import random
        return f"{base_name}_{random.randint(100000, 999999)}"

    def build_index(self, data_path: str, workers: int = 1) -> BenchmarkResult:
        """Build index from CSV data via ZincSearch bulk API."""
        self.index_name = self._get_unique_index_name(self.index_name)
        index_name = self.index_name

        # Create index with mapping using ES-compatible endpoint
        create_body = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "title": {"type": "text"},
                    "content": {"type": "text"},
                    "tags": {"type": "keyword"},
                    "views": {"type": "long"},
                    "published_at": {"type": "date"},
                    "author": {"type": "keyword"},
                }
            }
        }
        status, resp = self._request("PUT", f"/es/{index_name}", create_body)
        if status not in (200, 201) and resp.get("error"):
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(data_path),
                operation="build_index",
                rows=0,
                duration_ms=0,
                ops_per_sec=0,
                error=f"Failed to create index: {resp.get('error')}"
            )

        # Bulk index CSV data using ES bulk API format (NDJSON)
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
                    # Build NDJSON body: action line + doc line per document
                    lines = []
                    for doc in docs_batch:
                        doc_id_val = doc.get("id")
                        doc_id = str(doc_id_val) if doc_id_val is not None else str(rows_indexed)
                        # Strip None values - ZincSearch can't handle null fields
                        clean_doc = {k: v for k, v in doc.items() if v is not None}
                        lines.append(json.dumps({"index": {"_id": doc_id}}))
                        lines.append(json.dumps(clean_doc))
                    status, resp = self._request_ndjson("POST", f"/es/{index_name}/_bulk", lines)
                    if resp.get("error"):
                        return BenchmarkResult(
                            engine=self.name,
                            dataset=os.path.basename(data_path),
                            operation="build_index",
                            rows=rows_indexed,
                            duration_ms=(time.perf_counter() - start_time) * 1000,
                            ops_per_sec=0,
                            error=f"Import error: {resp.get('error')}"
                        )
                    docs_batch = []

                if rows_indexed % 50000 == 0:
                    print(f"  ZincSearch: {rows_indexed:,} docs indexed...")

        # Flush remaining docs
        if docs_batch:
            lines = []
            for doc in docs_batch:
                doc_id_val = doc.get("id")
                doc_id = str(doc_id_val) if doc_id_val is not None else str(rows_indexed)
                clean_doc = {k: v for k, v in doc.items() if v is not None}
                lines.append(json.dumps({"index": {"_id": doc_id}}))
                lines.append(json.dumps(clean_doc))
            self._request_ndjson("POST", f"/es/{index_name}/_bulk", lines)

        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = rows_indexed / (duration_ms / 1000) if duration_ms > 0 else 0

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
            memory_mb=0,
            metadata={"index_size_mb": 0}
        )

    def search(self, query: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run search queries via ZincSearch API."""
        index_name = self.index_name
        latencies = []
        result_counts = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["title^2", "content"]
                    }
                },
                "size": 1000
            }
            status, resp = self._request("POST", f"/es/{index_name}/_search", body)
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

    def aggregate(self, field: str, query: str = "*", agg_type: str = "terms", **kwargs) -> BenchmarkResult:
        """Run aggregation queries via ZincSearch API."""
        index_name = self.index_name

        start_time = time.perf_counter()

        # Build query
        if query != "*":
            q_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["title^2", "content"]
                    }
                },
                "size": 0,
                "aggs": {
                    "buckets": {
                        agg_type: {"field": field, "size": 100}
                    }
                }
            }
        else:
            q_body = {
                "query": {"match_all": {}},
                "size": 0,
                "aggs": {
                    "buckets": {
                        agg_type: {"field": field, "size": 100}
                    }
                }
            }

        status, resp = self._request("POST", f"/es/{index_name}/_search", q_body)
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0

        rows = 0
        if status == 200:
            aggs = resp.get("aggregations", {})
            # ZincSearch returns aggs keyed by agg name, value is {"buckets": [...]}
            # Find the first agg that has buckets
            for agg_name, agg_data in aggs.items():
                buckets = agg_data.get("buckets", [])
                rows = len(buckets)
                break

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

    def range_query(self, field: str, lo: int, hi: int, **kwargs) -> BenchmarkResult:
        """Run range queries via ZincSearch API."""
        index_name = self.index_name

        start_time = time.perf_counter()

        body = {
            "query": {
                "range": {
                    field: {
                        "gte": lo,
                        "lte": hi
                    }
                }
            },
            "size": 1000
        }

        status, resp = self._request("POST", f"/es/{index_name}/_search", body)
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0
        hits = 0

        if status == 200:
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
        """Run wildcard search queries via ZincSearch API."""
        index_name = self.index_name
        latencies = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            body = {
                "query": {
                    "wildcard": {
                        "title": f"*{pattern}*"
                    }
                },
                "size": 1000
            }
            self._request("POST", f"/es/{index_name}/_search", body)
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
        self._request("DELETE", f"/es/{self.index_name}")