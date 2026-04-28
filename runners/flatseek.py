"""Flatseek benchmark runner."""

import os
import sys
import time
import json
import tempfile
import shutil
import tracemalloc
import subprocess
from typing import Any
from pathlib import Path

from . import BaseRunner, BenchmarkResult, EngineConfig, register_engine

# Import flatseek core directly (avoid __init__.py which imports client.py with Python 3.10+ syntax)
FLATSEEK_SRC = os.path.join(os.path.dirname(__file__), "..", "..", "flatseek", "src")
sys.path.insert(0, FLATSEEK_SRC)

try:
    # Import core modules directly to avoid flatseek/__init__.py
    import importlib.util
    spec = importlib.util.spec_from_file_location("flatseek.core.builder",
        os.path.join(FLATSEEK_SRC, "flatseek", "core", "builder.py"))
    builder_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder_module)
    build = builder_module.build
    merge_worker_stats = builder_module.merge_worker_stats

    spec2 = importlib.util.spec_from_file_location("flatseek.core.query_engine",
        os.path.join(FLATSEEK_SRC, "flatseek", "core", "query_engine.py"))
    qe_module = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(qe_module)
    QueryEngine = qe_module.QueryEngine
    plan_fn = builder_module.plan

    FLATSEEK_AVAILABLE = True
except Exception as e:
    FLATSEEK_AVAILABLE = False
    build = None
    QueryEngine = None
    merge_worker_stats = None


@register_engine("flatseek")
class FlatseekRunner(BaseRunner):
    """Benchmark runner for Flatseek."""

    name = "flatseek"
    supports_aggregate = True
    supports_range_query = True
    supports_wildcard = True

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self._engine = None

    def _get_engine(self) -> QueryEngine:
        if self._engine is None:
            self._engine = QueryEngine(self.config.data_dir)
        return self._engine

    def build_index(self, data_path: str, workers: int = 1) -> BenchmarkResult:
        """Build index from CSV/JSONL data.

        Args:
            data_path: Path to CSV file.
            workers: Number of parallel workers for indexing. Default 1 (single-threaded).
        """
        if not FLATSEEK_AVAILABLE:
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(data_path),
                operation="build_index",
                rows=0,
                duration_ms=0,
                ops_per_sec=0,
                error="Flatseek not available"
            )

        tracemalloc.start()
        start_time = time.perf_counter()
        start_mem = tracemalloc.get_traced_memory()[0] / 1024 / 1024

        workers = max(1, workers)

        try:
            if workers > 1:
                # Parallel mode: generate plan in data_dir, then spawn CLI workers
                plan_fn(data_path, self.config.data_dir, workers)
                plan_path = os.path.join(self.config.data_dir, "build_plan.json")
                FLATSEEK_CLI = os.path.join(FLATSEEK_SRC, "flatseek", "cli.py")
                for w in range(workers):
                    cmd = [
                        sys.executable, FLATSEEK_CLI, "build", data_path,
                        "-o", self.config.data_dir,
                        "--plan", plan_path,
                        "--worker-id", str(w)
                    ]
                    subprocess.run(cmd, check=True)
                # Merge per-worker stats into final stats.json
                merge_worker_stats(self.config.data_dir, workers)
            else:
                # Single-threaded mode
                build(data_path, self.config.data_dir)
        except Exception as e:
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(data_path),
                operation="build_index",
                rows=0,
                duration_ms=(time.perf_counter() - start_time) * 1000,
                ops_per_sec=0,
                error=str(e)
            )

        end_time = time.perf_counter()
        end_mem = tracemalloc.get_traced_memory()[0] / 1024 / 1024
        tracemalloc.stop()

        # Count rows properly using csv.DictReader to handle embedded newlines
        import csv as csv_mod
        with open(data_path, newline="") as f:
            reader = csv_mod.DictReader(f)
            rows = sum(1 for _ in reader)

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = rows / (duration_ms / 1000) if duration_ms > 0 else 0

        # Get stats
        engine = self._get_engine()
        stats = engine.stats
        memory_mb = end_mem - start_mem

        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(data_path),
            operation="build_index",
            rows=rows,
            duration_ms=duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=duration_ms,  # single operation
            latency_p95_ms=duration_ms,
            latency_p99_ms=duration_ms,
            memory_mb=memory_mb,
            metadata={
                "index_files": stats.get("index_files", 0),
                "index_size_mb": stats.get("index_size_mb", 0),
                "docstore_size_mb": stats.get("docs_size_mb", 0),
                "columns_count": len(stats.get("columns", {})),
            }
        )

    def search(self, query: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run search queries."""
        if not FLATSEEK_AVAILABLE:
            return BenchmarkResult(
                engine=self.name,
                dataset="",
                operation="search",
                rows=0,
                duration_ms=0,
                ops_per_sec=0,
                error="Flatseek not available"
            )

        engine = self._get_engine()
        latencies = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            result = engine.query(query)
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
            operation="search",
            rows=iterations,
            duration_ms=total_duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=p50,
            latency_p95_ms=p95,
            latency_p99_ms=p99,
            metadata={"query": query, "result_count": len(result.get("results", []))}
        )

    def aggregate(self, field: str, query: str = "*", agg_type: str = "terms", **kwargs) -> BenchmarkResult:
        """Run aggregation queries."""
        if not FLATSEEK_AVAILABLE:
            return BenchmarkResult(
                engine=self.name,
                dataset="",
                operation="aggregate",
                rows=0,
                duration_ms=0,
                ops_per_sec=0,
                error="Flatseek not available"
            )

        engine = self._get_engine()

        start_time = time.perf_counter()
        aggs = {agg_type: {"field": field, "size": 100}}
        result = engine.aggregate(q=query, aggs=aggs)
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0

        # For terms: buckets. For stats/min/max/sum: value. cardinality: value
        aggs_result = result.get("aggregations", {})
        key = field  # flatseek returns keyed by field name
        agg_result = aggs_result.get(key, aggs_result.get(agg_type, {}))
        if agg_type == "terms":
            buckets = agg_result.get("buckets", [])
            rows = len(buckets)
        else:
            rows = 1  # stats/min/max/sum/cardinality return single value
            _ = agg_result  # avoid unused warning

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
        if not FLATSEEK_AVAILABLE:
            return BenchmarkResult(
                engine=self.name,
                dataset="",
                operation="range_query",
                rows=0,
                duration_ms=0,
                ops_per_sec=0,
                error="Flatseek not available"
            )

        engine = self._get_engine()
        query = f"{field}:[{lo} TO {hi}]"

        start_time = time.perf_counter()
        result = engine.query(query)
        end_time = time.perf_counter()

        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0

        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(self.config.data_dir),
            operation="range_query",
            rows=result.get("total", 0),
            duration_ms=duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=duration_ms,
            latency_p95_ms=duration_ms,
            latency_p99_ms=duration_ms,
            metadata={"query": query, "result_count": result.get("total", 0)}
        )

    def wildcard_search(self, pattern: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run wildcard search queries."""
        if not FLATSEEK_AVAILABLE:
            return BenchmarkResult(
                engine=self.name,
                dataset="",
                operation="wildcard_search",
                rows=0,
                duration_ms=0,
                ops_per_sec=0,
                error="Flatseek not available"
            )

        engine = self._get_engine()
        latencies = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            result = engine.query(f"*{pattern}*")
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
        """Clean up resources."""
        self._engine = None