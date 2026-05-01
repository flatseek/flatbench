"""Flatseek benchmark runner."""

import os
import sys
import time
import json
import csv
import tempfile
import shutil
import tracemalloc
import subprocess
import shutil as shutil_mod
from typing import Any
from pathlib import Path

from . import BaseRunner, BenchmarkResult, EngineConfig, register_engine

# Strategy:
# 1. Try to use `flatseek` CLI (installed via pip, uses pyenv Python 3.10+)
# 2. Fallback: import flatseek from pip if Python >= 3.10
# 3. Last resort: import from local source tree (development mode)

def _find_flatseek_cli():
    """Find flatseek CLI binary in PATH."""
    # Prefer the flatseek source tree's CLI (v0.1.2) over other installations
    local_cli = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "flatseek", "env", "bin", "flatseek")
    if os.path.exists(local_cli):
        return local_cli
    return shutil_mod.which("flatseek")

FLATSEEK_CLI = _find_flatseek_cli()
FLATSEEK_AVAILABLE = FLATSEEK_CLI is not None

# Try to import query engine for search operations (works alongside CLI mode)
_query_engine_class = None

if sys.version_info >= (3, 10):
    try:
        from flatseek.core.query_engine import QueryEngine as QEC
        _query_engine_class = QEC
    except Exception:
        pass

if _query_engine_class is None:
    # Development mode or Python < 3.10: import from local source tree
    # __file__ is src/flatbench/runners/flatseek_cli.py
    # up 4 levels: runners → flatbench/src/flatbench → flatbench/src → flatbench → flatseek
    FLATSEEK_SRC = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "flatseek", "src")
    if os.path.exists(FLATSEEK_SRC):
        import importlib.util
        sys.path.insert(0, FLATSEEK_SRC)
        try:
            spec2 = importlib.util.spec_from_file_location("flatseek.core.query_engine",
                os.path.join(FLATSEEK_SRC, "flatseek", "core", "query_engine.py"))
            qe_module = importlib.util.module_from_spec(spec2)
            spec2.loader.exec_module(qe_module)
            _query_engine_class = qe_module.QueryEngine
        except Exception:
            pass


@register_engine("flatseek_cli")
class FlatseekCliRunner(BaseRunner):
    """Benchmark runner for Flatseek via CLI (direct Python API).

    Uses `flatseek` CLI (pip-installed) for building index.
    Uses Python QueryEngine API for search/aggregate queries.
    """

    name = "flatseek_cli"
    supports_aggregate = True
    supports_range_query = True
    supports_wildcard = True

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self._engine = None

    def _get_engine(self):
        if self._engine is None:
            self._engine = _query_engine_class(self.config.data_dir)
        return self._engine

    def build_index(self, data_path: str, workers: int = 1) -> BenchmarkResult:
        """Build index from CSV/JSONL data.

        Uses `flatseek build` CLI when available (pip-installed).
        Falls back to Python API when CLI is not in PATH.

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
            if FLATSEEK_CLI:
                # CLI mode: use pip-installed flatseek CLI.
                # NOTE: do NOT pass --columns even if we detect them from DictReader.
                # flatseek's --columns implies "first row = data" (headerless), which
                # breaks correctly-formed CSV files that have a header row.
                #
                # Use stdin=DEVNULL to suppress the interactive "Is Row 1 the column header?" prompt.
                # When stdin is not a TTY, flatseek auto-assumes row 1 is the header (no prompt).
                cmd = [FLATSEEK_CLI, "build", data_path, "-o", self.config.data_dir]
                if workers > 1:
                    cmd.extend(["--workers", str(workers)])
                result = subprocess.run(cmd, check=True, stdin=subprocess.DEVNULL)
            else:
                # Python API mode (when CLI not in PATH)
                from flatseek.core.builder import build as api_build, plan as api_plan, merge_worker_stats as api_merge
                if workers > 1:
                    api_plan(data_path, self.config.data_dir, workers)
                    plan_path = os.path.join(self.config.data_dir, "build_plan.json")
                    for w in range(workers):
                        cmd = [
                            sys.executable, "-c",
                            f"from flatseek.core.builder import build; build('{data_path}', '{self.config.data_dir}', plan_path='{plan_path}', worker_id={w})"
                        ]
                        subprocess.run(cmd, check=True)
                    api_merge(self.config.data_dir, workers)
                else:
                    api_build(data_path, self.config.data_dir)
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
            latency_p50_ms=duration_ms,
            latency_p95_ms=duration_ms,
            latency_p99_ms=duration_ms,
            memory_mb=memory_mb,
            metadata={
                "index_files": stats.get("index_files", 0),
                "index_size_mb": stats.get("index_size_mb", 0),
                "docstore_size_mb": stats.get("docs_size_mb", 0),
                "columns_count": len(stats.get("columns", {})),
                "mode": "cli" if FLATSEEK_CLI else "api",
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