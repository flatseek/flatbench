"""Benchmark suite runner."""

import os
import sys
import time
import json
import argparse
import tempfile
import shutil
from datetime import datetime
from typing import Any

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runners import get_engine, list_engines, EngineConfig, BenchmarkResult
from generators import generate_dataset, SCHEMAS


def _count_rows(data_path: str) -> int:
    """Count rows in CSV/JSONL file (excludes header)."""
    try:
        if data_path.endswith(".jsonl"):
            with open(data_path) as f:
                return sum(1 for _ in f)
        else:
            # Must use csv reader to handle embedded newlines in quoted fields
            import csv
            with open(data_path, newline="") as f:
                reader = csv.DictReader(f)
                return sum(1 for _ in reader)
    except Exception:
        return 0


def _read_sample_row(data_path: str) -> dict:
    """Read first data row for report sample."""
    try:
        if data_path.endswith(".jsonl"):
            import json
            with open(data_path) as f:
                first = f.readline()
                if first:
                    return json.loads(first)
        else:
            import csv
            with open(data_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    return dict(row)
    except Exception:
        return {}


class BenchmarkSuite:
    """Orchestrates benchmark runs across multiple engines and datasets."""

    def __init__(self, output_dir: str = "./output", run_meta: dict = None):
        self.output_dir = output_dir
        self.results: list[BenchmarkResult] = []
        self.run_meta = run_meta or {}  # e.g. {schema, workers, mode, format, source}
        self._engines_tested: set = set()
        self._engine_versions: dict = {}
        os.makedirs(output_dir, exist_ok=True)

    @property
    def engines(self) -> set:
        return self._engines_tested

    def _detect_engine_versions(self) -> dict:
        """Detect version of each engine."""
        versions = {}
        # flatseek
        try:
            import sys, os
            FLATSEEK_SRC = os.path.join(os.path.dirname(__file__), "..", "flatseek", "src")
            sys.path.insert(0, FLATSEEK_SRC)
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "flatseek.version",
                os.path.join(FLATSEEK_SRC, "flatseek", "__init__.py")
            )
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                versions["flatseek"] = getattr(mod, "__version__", "unknown")
            else:
                versions["flatseek"] = "unknown"
        except Exception:
            versions["flatseek"] = "unknown"

        # elasticsearch
        try:
            import elasticsearch
            versions["elasticsearch"] = elasticsearch.__version__
        except Exception:
            versions["elasticsearch"] = "unknown"

        # python
        import platform
        versions["python"] = f"{platform.python_version()} ({platform.platform()})"
        return versions

    def run_benchmark(
        self,
        engine_name: str,
        data_path: str,
        config: EngineConfig,
        queries: list[dict],
        aggregate_aggs: list[dict],
        range_tests: list[dict],
        wildcard_tests: list[str],
        iterations: int = 10,
        workers: int = 1,
    ) -> list[BenchmarkResult]:
        """Run a full benchmark suite for one engine on one dataset."""
        self._engines_tested.add(engine_name)
        print(f"\n{'='*60}")
        print(f"Benchmarking {engine_name} on {os.path.basename(data_path)}" +
              (f" ({workers} workers)" if workers > 1 else ""))
        print(f"{'='*60}")

        # Import all runners to register them
        from runners import flatseek, sqlite, elasticsearch  # noqa

        runner_class = get_engine(engine_name)
        runner = runner_class(config)

        results = []

        # Build index (workers only supported for flatseek)
        print(f"\n[1/5] Building index..." + (f" ({workers} workers)..." if workers > 1 else "..."))
        if engine_name == "flatseek" and workers > 1:
            build_result = runner.build_index(data_path, workers=workers)
        else:
            build_result = runner.build_index(data_path)
        results.append(build_result)
        print(f"  Duration: {build_result.duration_ms:.2f}ms")
        print(f"  Rows/sec: {build_result.ops_per_sec:.2f}")
        if build_result.error:
            print(f"  ERROR: {build_result.error}")
            return results

        # Search queries
        print(f"\n[2/5] Running search queries ({iterations} iterations each)...")
        for q in queries:
            query = q.get("query", "*")
            label = q.get("label", query)
            result = runner.search(query, iterations=iterations)
            result.metadata["label"] = label
            results.append(result)
            print(f"  [{label}] p50={result.latency_p50_ms:.3f}ms, p95={result.latency_p95_ms:.3f}ms, ops/s={result.ops_per_sec:.2f}")

        # Wildcard searches
        if runner.supports_wildcard:
            print(f"\n[3/5] Running wildcard searches...")
            for pattern in wildcard_tests:
                result = runner.wildcard_search(pattern, iterations=iterations)
                result.metadata["label"] = f"wildcard:{pattern}"
                results.append(result)
                print(f"  [{pattern}] p50={result.latency_p50_ms:.3f}ms, p95={result.latency_p95_ms:.3f}ms")

        # Range queries
        if runner.supports_range_query:
            print(f"\n[4/5] Running range queries...")
            for rt in range_tests:
                field = rt.get("field")
                lo = rt.get("lo")
                hi = rt.get("hi")
                result = runner.range_query(field, lo, hi)
                result.metadata["label"] = f"{field}:[{lo} TO {hi}]"
                results.append(result)
                print(f"  [{field}:{lo}-{hi}] hits={result.rows}, duration={result.duration_ms:.3f}ms")

        # Aggregations
        if runner.supports_aggregate:
            print(f"\n[5/5] Running aggregations...")
            for agg in aggregate_aggs:
                field = agg["field"]
                agg_type = agg.get("type", "terms")
                label = agg.get("label", field)
                result = runner.aggregate(field, agg_type=agg_type)
                result.metadata["label"] = f"agg:{label}"
                result.metadata["agg_type"] = agg_type
                results.append(result)
                bucket_info = f"buckets={result.rows}" if agg_type == "terms" else f"type={agg_type}"
                print(f"  [{label} / {agg_type}] {bucket_info}, duration={result.duration_ms:.3f}ms")

        runner.cleanup()
        self.results.extend(results)
        return results

    def generate_report(self, filename: str = None) -> str:
        """Generate detailed benchmark report."""
        if filename is None:
            filename = f"benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        output_path = os.path.join(self.output_dir, filename)

        report = {
            "generated_at": datetime.now().isoformat(),
            "total_results": len(self.results),
            "run_meta": self.run_meta,
            "summary": {},
            "details": [r.to_dict() for r in self.results],
        }

        # Calculate per-engine summary
        engines = set(r.engine for r in self.results)
        for eng in engines:
            eng_results = [r for r in self.results if r.engine == eng]
            operations = set(r.operation for r in eng_results)

            eng_summary = {}
            for op in operations:
                op_results = [r for r in eng_results if r.operation == op]
                durations = [r.duration_ms for r in op_results if r.duration_ms > 0]
                ops_per_sec = [r.ops_per_sec for r in op_results if r.ops_per_sec > 0]

                eng_summary[op] = {
                    "count": len(op_results),
                    "avg_duration_ms": sum(durations) / len(durations) if durations else 0,
                    "avg_ops_per_sec": sum(ops_per_sec) / len(ops_per_sec) if ops_per_sec else 0,
                    "errors": sum(1 for r in op_results if r.error),
                }

            report["summary"][eng] = eng_summary

        # Also generate markdown table
        md_path = output_path.replace(".json", ".md")
        with open(md_path, "w") as f:
            f.write("# Benchmark Results\n\n")
            f.write(f"Generated: {report['generated_at']}\n\n")

            # ── Benchmark Configuration ─────────────────────────────────────────────
            f.write("## Benchmark Configuration\n\n")
            meta = self.run_meta
            rows = meta.get("rows", 0)
            schema = meta.get("schema", "unknown")
            workers = meta.get("workers", 1)
            mode = meta.get("mode", "normal")
            fmt = meta.get("source_format", "csv")
            source = meta.get("source_path", "")

            # Detect storage type
            storage_type = "unknown"
            if mode == "tmpfs":
                storage_type = "tmpfs (RAM disk)"
            elif source and os.path.exists(source):
                storage_type = "file (SSD)"
            else:
                storage_type = "temp dir (SSD)"

            # Dataset file size
            dataset_file_size_mb = ""
            stored_size = meta.get("data_file_size_mb", "")
            if stored_size != "":
                dataset_file_size_mb = f"{stored_size:.1f}"
            else:
                source = meta.get("source_path", "")
                data_path = meta.get("data_path", "")
                if source and os.path.exists(source):
                    dataset_file_size_mb = f"{os.path.getsize(source) / 1024 / 1024:.1f}"
                elif data_path and os.path.exists(data_path):
                    dataset_file_size_mb = f"{os.path.getsize(data_path) / 1024 / 1024:.1f}"
                else:
                    dataset_file_size_mb = "N/A"

            # Engine versions
            engine_versions = self._detect_engine_versions()

            f.write(f"| Parameter | Value |\n")
            f.write(f"|-----------|-------|\n")
            f.write(f"| Schema | {schema} |\n")
            f.write(f"| Dataset rows | {rows:,} |\n")
            f.write(f"| Dataset file size (MB) | {dataset_file_size_mb} |\n")
            f.write(f"| Workers (flatseek) | {workers} |\n")
            f.write(f"| Index mode | {mode} |\n")
            f.write(f"| Storage type | {storage_type} |\n")
            f.write(f"| Data format | {fmt} |\n")
            if source:
                f.write(f"| Data source | {source} |\n")
            else:
                f.write(f"| Data source | generated |\n")
            f.write(f"| Engines tested | {', '.join(sorted(self.engines))} |\n")
            f.write(f"| Iterations per query | {meta.get('iterations', 10)} |\n")
            for eng, ver in sorted(engine_versions.items()):
                f.write(f"| {eng} version | {ver} |\n")

            # Sample row
            sample = meta.get("sample_row", {})
            if sample:
                f.write(f"\n**Sample data row (1 of {rows:,}):**\n\n")
                f.write("| Field | Value |\n")
                f.write("|-------|-------|\n")
                for k, v in list(sample.items()):
                    vstr = str(v)[:120] + "..." if len(str(v)) > 120 else str(v)
                    f.write(f"| {k} | {vstr} |\n")
                f.write("\n")

            # Build summary data for comparison
            summary_data = {}
            for eng in engines:
                eng_results = [r for r in self.results if r.engine == eng]
                build = next((r for r in eng_results if r.operation == "build_index"), None)
                search_results = [r for r in eng_results if r.operation == "search"]
                wildcard_results = [r for r in eng_results if r.operation == "wildcard_search"]
                range_results = [r for r in eng_results if r.operation == "range_query"]
                agg_results = [r for r in eng_results if r.operation == "aggregate"]

                search_p50s = sorted(r.latency_p50_ms for r in search_results if r.latency_p50_ms > 0)
                wildcard_p50s = sorted(r.latency_p50_ms for r in wildcard_results if r.latency_p50_ms > 0)
                range_hits = sum(r.rows for r in range_results if not r.error)
                range_errors = sum(1 for r in range_results if r.error)
                agg_buckets = sum(r.rows for r in agg_results if not r.error)
                agg_errors = sum(1 for r in agg_results if r.error)

                summary_data[eng] = {
                    "build_ms": build.duration_ms if build else 0,
                    "build_rows_sec": build.ops_per_sec if build else 0,
                    "search_p50_ms": search_p50s[len(search_p50s)//2] if search_p50s else 0,
                    "search_p95_ms": search_p50s[int(len(search_p50s)*0.95)] if search_p50s else 0,
                    "wildcard_p50_ms": wildcard_p50s[len(wildcard_p50s)//2] if wildcard_p50s else 0,
                    "range_hits": range_hits,
                    "range_errors": range_errors,
                    "agg_buckets": agg_buckets,
                    "agg_errors": agg_errors,
                }

            # Key insights — right after config
            f.write("## Key Insights\n\n")
            build_times = {eng: summary_data[eng]["build_ms"] for eng in engines if summary_data[eng]["build_ms"] > 0}
            if build_times:
                fastest = min(build_times, key=build_times.get)
                slowest = max(build_times, key=build_times.get)
                ratio = build_times[slowest] / build_times[fastest] if build_times[fastest] > 0 else 0
                f.write(f"- **Build**: {fastest} fastest ({build_times[fastest]:.0f}ms). {slowest} is {ratio:.0f}x slower.\n")
            search_times = {eng: summary_data[eng]["search_p50_ms"] for eng in engines if summary_data[eng]["search_p50_ms"] > 0}
            if search_times:
                fastest = min(search_times, key=search_times.get)
                slowest = max(search_times, key=search_times.get)
                ratio = search_times[slowest] / search_times[fastest] if search_times[fastest] > 0 else 0
                f.write(f"- **Search**: {fastest} fastest (p50 {search_times[fastest]:.3f}ms). {slowest} is {ratio:.0f}x slower.\n")
            range_engines = {eng: summary_data[eng]["range_hits"] for eng in engines if summary_data[eng]["range_errors"] == 0}
            if range_engines:
                f.write(f"- **Range queries**: {', '.join(f'{k}({v} hits)' for k,v in sorted(range_engines.items()))}.\n")
            agg_engines = {eng: summary_data[eng]["agg_buckets"] for eng in engines if summary_data[eng]["agg_errors"] == 0}
            if agg_engines:
                f.write(f"- **Aggregations**: {', '.join(f'{k}({v} buckets)' for k,v in sorted(agg_engines.items()))}.\n")
            f.write("\n")

            # ── Build Index (comparable) ─────────────────────────────────────────
            f.write("### Build Index\n\n")
            f.write("| Engine | Duration (ms) | Rows/sec | Index size (MB) | RSS delta (MB) | Winner |\n")
            f.write("|--------|---------------|----------|-----------------|----------------|--------|\n")
            build_results = [r for r in self.results if r.operation == "build_index"]
            if build_results:
                by_eng = {r.engine: r for r in build_results}
                flat_build = by_eng.get("flatseek")
                es_build = by_eng.get("elasticsearch")
                for eng in sorted(by_eng.keys()):
                    r = by_eng[eng]
                    if r.error:
                        f.write(f"| {eng} | ERROR | - | - | - | - |\n")
                    else:
                        idx_mb = r.metadata.get("index_size_mb", 0) or r.memory_mb or 0
                        # RSS delta: ES uses es_rss_delta_mb, flatseek uses tracemalloc memory_mb
                        if eng == "elasticsearch":
                            rss_delta = r.metadata.get("es_rss_delta_mb", 0)
                        else:
                            rss_delta = r.memory_mb or 0
                        winner = ""
                        if flat_build and es_build and not flat_build.error and not es_build.error:
                            if r.duration_ms == min(flat_build.duration_ms, es_build.duration_ms):
                                winner = " ◀"
                        f.write(f"| {eng} | {r.duration_ms:.0f} | {r.ops_per_sec:.0f} | {idx_mb:.1f} | {rss_delta:.1f} |{winner}\n")
            f.write("\n")

            # ── Search (comparable) ────────────────────────────────────────────
            search_ops = [r for r in self.results if r.operation == "search"]
            if search_ops:
                f.write("### Search\n\n")
                f.write("| Engine | p50 (ms) | p95 (ms) | p99 (ms) | Ops/sec | Queries | Winner |\n")
                f.write("|--------|----------|----------|----------|---------|---------|--------|\n")
                for eng in sorted(set(r.engine for r in search_ops)):
                    eng_search = [r for r in search_ops if r.engine == eng]
                    if eng_search:
                        p50s = sorted(r.latency_p50_ms for r in eng_search)
                        p95s = sorted(r.latency_p95_ms for r in eng_search)
                        p99s = sorted(r.latency_p99_ms for r in eng_search)
                        ops = sorted(r.ops_per_sec for r in eng_search if r.ops_per_sec > 0)
                        n = len(p50s)
                        p50_med = p50s[n//2]
                        winner = ""
                        # Compare p50 with other engine
                        other_eng_search = [r for r in search_ops if r.engine != eng]
                        if other_eng_search:
                            other_p50s = sorted(r.latency_p50_ms for r in other_eng_search)
                            other_n = len(other_p50s)
                            other_med = other_p50s[other_n // 2]
                            if p50_med < other_med:
                                winner = " ◀"
                        f.write(f"| {eng} | {p50_med:.3f} | {p95s[int(n*0.95)]:.3f} | {p99s[int(n*0.99)]:.3f} | {ops[n//2] if ops else 0:.0f} | {n} |{winner}\n")
                f.write("\n")

            # ── Wildcard (comparable) ──────────────────────────────────────────
            wc_ops = [r for r in self.results if r.operation == "wildcard_search"]
            if wc_ops:
                f.write("### Wildcard Search\n\n")
                f.write("| Engine | p50 (ms) | p95 (ms) | Ops/sec | Patterns | Winner |\n")
                f.write("|--------|----------|----------|---------|----------|--------|\n")
                for eng in sorted(set(r.engine for r in wc_ops)):
                    eng_wc = [r for r in wc_ops if r.engine == eng]
                    if eng_wc:
                        p50s = sorted(r.latency_p50_ms for r in eng_wc)
                        p95s = sorted(r.latency_p95_ms for r in eng_wc)
                        ops = sorted(r.ops_per_sec for r in eng_wc if r.ops_per_sec > 0)
                        n = len(p50s)
                        p50_med = p50s[n//2]
                        winner = ""
                        other_wc = [r for r in wc_ops if r.engine != eng]
                        if other_wc:
                            other_p50s = sorted(r.latency_p50_ms for r in other_wc)
                            other_med = other_p50s[len(other_p50s)//2]
                            if p50_med < other_med:
                                winner = " ◀"
                        f.write(f"| {eng} | {p50_med:.3f} | {p95s[int(n*0.95)]:.3f} | {ops[n//2] if ops else 0:.0f} | {n} |{winner}\n")
                f.write("\n")

            # ── Range Query (comparable) ──────────────────────────────────────
            range_ops = [r for r in self.results if r.operation == "range_query"]
            if range_ops:
                f.write("### Range Query\n\n")
                f.write("| Engine | Total hits | Avg duration (ms) | Queries | Winner |\n")
                f.write("|--------|-----------|------------------|---------|--------|\n")
                for eng in sorted(set(r.engine for r in range_ops)):
                    eng_range = [r for r in range_ops if r.engine == eng]
                    if eng_range:
                        total_hits = sum(r.rows for r in eng_range if not r.error)
                        durations = [r.duration_ms for r in eng_range if not r.error and r.duration_ms > 0]
                        avg_dur = sum(durations) / len(durations) if durations else 0
                        winner = ""
                        other_range = [r for r in range_ops if r.engine != eng]
                        if other_range:
                            other_durations = [r.duration_ms for r in other_range if not r.error and r.duration_ms > 0]
                            if other_durations:
                                other_avg = sum(other_durations) / len(other_durations)
                                if avg_dur < other_avg:
                                    winner = " ◀"
                        f.write(f"| {eng} | {total_hits:,} | {avg_dur:.2f} | {len(eng_range)} |{winner}\n")
                f.write("\n")

            # ── Aggregate (comparable) ────────────────────────────────────────
            agg_ops = [r for r in self.results if r.operation == "aggregate"]
            if agg_ops:
                f.write("### Aggregation\n\n")

                # Group by label to pair same aggs across engines
                from collections import defaultdict
                agg_by_label = defaultdict(list)
                for r in agg_ops:
                    label = r.metadata.get("label", "")
                    agg_by_label[label].append(r)

                # ── Single comparable table ───────────────────────────────────
                f.write("| Aggregation | Type | flatseek (ms) | elasticsearch (ms) | Winner |\n")
                f.write("|-------------|------|--------------|---------------------|--------|\n")

                for label in sorted(agg_by_label.keys()):
                    results_for_label = agg_by_label[label]
                    by_eng = {r.engine: r for r in results_for_label}

                    flat_r = by_eng.get("flatseek")
                    es_r = by_eng.get("elasticsearch")
                    agg_type = flat_r.metadata.get("agg_type", "terms") if flat_r else "terms"
                    agg_label = label.replace("agg:", "")

                    flat_dur = "N/A"
                    es_dur = "N/A"
                    winner = "-"
                    if flat_r and not flat_r.error and es_r and not es_r.error:
                        flat_dur = f"{flat_r.duration_ms:.2f}"
                        es_dur = f"{es_r.duration_ms:.2f}"
                        if flat_r.duration_ms < es_r.duration_ms:
                            winner = "◀ flatseek"
                        elif es_r.duration_ms < flat_r.duration_ms:
                            winner = "◀ elasticsearch"
                        else:
                            winner = "tie"
                    elif flat_r and not flat_r.error:
                        flat_dur = f"{flat_r.duration_ms:.2f}"
                        winner = "flatseek (only)"
                    elif es_r and not es_r.error:
                        es_dur = f"{es_r.duration_ms:.2f}"
                        winner = "elasticsearch (only)"

                    f.write(f"| {agg_label} | {agg_type} | {flat_dur} | {es_dur} | {winner} |\n")
                f.write("\n")

            # ── Performance Analysis ────────────────────────────────────────────
            def _write_op_analysis_table(title: str, ops: list, engines: set):
                if not ops:
                    return

                # Group by label across engines
                from collections import defaultdict
                by_label = defaultdict(list)
                for r in ops:
                    label = r.metadata.get("label", "")
                    by_label[label].append(r)

                rows_out = []
                for label in sorted(by_label.keys()):
                    results = by_label[label]
                    by_eng = {r.engine: r for r in results}
                    if len(by_eng) < 2:
                        continue  # skip single-engine only

                    flat_r = by_eng.get("flatseek")
                    es_r = by_eng.get("elasticsearch")
                    if not flat_r or not es_r:
                        continue
                    if flat_r.error or es_r.error:
                        continue

                    flat_p50 = flat_r.latency_p50_ms
                    es_p50 = es_r.latency_p50_ms
                    flat_p95 = flat_r.latency_p95_ms
                    es_p95 = es_r.latency_p95_ms

                    loser_p50 = ""
                    loser_p95 = ""
                    loser_desc = ""

                    if flat_p50 > es_p50:
                        loser_p50 = "flatseek"
                        ratio_p50 = flat_p50 / es_p50 if es_p50 > 0 else float("inf")
                    else:
                        loser_p50 = "elasticsearch"
                        ratio_p50 = es_p50 / flat_p50 if flat_p50 > 0 else float("inf")

                    if flat_p95 > es_p95:
                        loser_p95 = "flatseek"
                        ratio_p95 = flat_p95 / es_p95 if es_p95 > 0 else float("inf")
                    else:
                        loser_p95 = "elasticsearch"
                        ratio_p95 = es_p95 / flat_p95 if flat_p95 > 0 else float("inf")

                    if loser_p50 == loser_p95:
                        loser_desc = f"{loser_p50} loses on both p50 ({ratio_p50:.1f}x) and p95 ({ratio_p95:.1f}x)"
                    else:
                        loser_desc = f"{loser_p50} loses p50 ({ratio_p50:.1f}x), {loser_p95} loses p95 ({ratio_p95:.1f}x)"

                    # Clean label for display
                    clean_label = label.replace("wildcard:", "pattern:").replace(":", " ")

                    rows_out.append({
                        "query": clean_label,
                        "flat_p50": flat_p50,
                        "es_p50": es_p50,
                        "flat_p95": flat_p95,
                        "es_p95": es_p95,
                        "loser": loser_p50,
                        "desc": loser_desc,
                    })

                if not rows_out:
                    return

                f.write(f"### {title}\n\n")
                f.write("| Query | flatseek p50 | ES p50 | flatseek p95 | ES p95 | Loser | Note |\n")
                f.write("|-------|-------------|--------|-------------|--------|-------|------|\n")
                for row in rows_out:
                    f.write(f"| `{row['query']}` | {row['flat_p50']:.2f}ms | {row['es_p50']:.2f}ms | {row['flat_p95']:.2f}ms | {row['es_p95']:.2f}ms | {row['loser']} | {row['desc']} |\n")
                f.write("\n")

            # Search analysis
            _write_op_analysis_table("Search (per query)", [r for r in self.results if r.operation == "search"], engines)

            # Wildcard analysis
            _write_op_analysis_table("Wildcard Search (per pattern)", [r for r in self.results if r.operation == "wildcard_search"], engines)

            # Range analysis
            _write_op_analysis_table("Range Query (per field)", [r for r in self.results if r.operation == "range_query"], engines)

            # Aggregate analysis
            _write_op_analysis_table("Aggregation (per field/type)", [r for r in self.results if r.operation == "aggregate"], engines)

        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)

        print(f"\nReport saved to:")
        print(f"  JSON: {output_path}")
        print(f"  Markdown: {md_path}")

        return output_path

    def print_summary(self):
        """Print a quick summary table."""
        engines = set(r.engine for r in self.results)
        print("\n" + "="*80)
        print(f"{'ENGINE':<15} {'OPERATION':<20} {'ROWS':<10} {'DURATION':<15} {'OPS/SEC':<12} {'P50':<10}")
        print("="*80)

        for eng in sorted(engines):
            eng_results = [r for r in self.results if r.engine == eng]
            for r in sorted(eng_results, key=lambda x: x.operation):
                if r.error:
                    print(f"{eng:<15} {r.operation:<20} {'ERROR':<10} {'-':<15} {'-':<12} {'-':<10}")
                else:
                    print(f"{eng:<15} {r.operation:<20} {r.rows:<10} {r.duration_ms:<15.2f} {r.ops_per_sec:<12.2f} {r.latency_p50_ms:<10.3f}")
            print("-"*80)


def run_compare(
    engines: list[str],
    sizes: list[int],
    schema: str = "standard",
    workers: int = 1,
    source_format: str = "csv",
    source_path: str = "",
    mode: str = "normal",
):
    """Compare multiple engines across different dataset sizes.

    Args:
        engines: List of engine names to compare.
        sizes: List of dataset sizes to test.
        schema: Schema name for data generation.
        workers: Number of parallel workers for flatseek indexing. Default 1.
        source_format: Format of source data (csv or jsonl).
        source_path: If set, use this file as data source instead of generating.
        mode: Index storage mode ("normal" or "tmpfs").
    """
    suite = BenchmarkSuite(run_meta={
        "schema": schema,
        "workers": workers,
        "mode": mode,
        "source_format": source_format,
        "source_path": source_path,
        "sizes": list(sizes),
        "engines": list(engines),
        "iterations": 10,
    })

    # Determine index base directory based on mode
    if mode == "tmpfs":
        # Try multiple tmpfs mount points in order of preference
        for candidate in ["/Volumes/RAMDisk", "/mnt/tmpfs_flatbench", "/tmp/flatbench_tmpfs"]:
            if os.path.exists(candidate):
                index_base = candidate
                print(f"[MODE: tmpfs] Index base: {index_base}")
                break
        else:
            index_base = None
            print(f"[MODE: tmpfs] No tmpfs mount found — falling back to temp dirs")
    else:
        index_base = None  # use temp dirs

    for size in sizes:
        print(f"\n\n{'#'*60}")
        print(f"# DATASET SIZE: {size:,} rows")
        print(f"{'#'*60}")

        # Data source: use source_path if provided, else generate
        with tempfile.TemporaryDirectory() as tmpdir:
            if source_path:
                import shutil
                src = source_path
                if os.path.isdir(src):
                    candidates = [f for f in os.listdir(src)
                                 if (source_format == "csv" and f.endswith(".csv")) or
                                    (source_format == "jsonl" and f.endswith(".jsonl"))]
                    if not candidates:
                        raise FileNotFoundError(f"No .{source_format} files in {src}")
                    src = os.path.join(src, candidates[0])
                data_path = os.path.join(tmpdir, f"data_{size}.{source_format}")
                shutil.copy2(src, data_path)
                print(f"Using source: {src}")
            else:
                data_path = os.path.join(tmpdir, f"data_{size}.{source_format}")
                print(f"Generating {size:,} rows with schema '{schema}'...")
                generate_dataset(schema, size, data_path, source_format)

            # Extract sample row for report metadata
            sample_row = _read_sample_row(data_path)
            if sample_row:
                suite.run_meta["sample_row"] = sample_row
                # Also store rows in meta (might differ from size if source was used)
                suite.run_meta["rows"] = _count_rows(data_path)
                suite.run_meta["data_path"] = data_path
                suite.run_meta["data_file_size_mb"] = os.path.getsize(data_path) / 1024 / 1024

            for engine_name in engines:
                if index_base:
                    engine_index_dir = os.path.join(index_base, f"index_{size}_{engine_name}")
                    os.makedirs(engine_index_dir, exist_ok=True)
                else:
                    engine_index_dir = os.path.join(tmpdir, f"index_{engine_name}")

                config = EngineConfig(
                    name=engine_name,
                    data_dir=engine_index_dir,
                )

                # Schema-specific queries — comprehensive coverage matching test.py
                if schema == "nested":
                    # Array field: preferences.tags (expanded to preferences.tags[0], etc.)
                    # Object fields: profile.location.city, metadata.id, metadata.metadata.tags
                    # Deeply nested: metadata.metadata.tags[0], metadata.metadata.created
                    queries = [
                        # Match-all
                        {"query": "*", "label": "match_all"},
                        # Array field - tags (preferences.tags contains random tags)
                        {"query": "preferences.tags:python", "label": "array_tags_python"},
                        {"query": "preferences.tags:golang", "label": "array_tags_golang"},
                        {"query": "preferences.tags:docker", "label": "array_tags_docker"},
                        {"query": "preferences.tags:react", "label": "array_tags_react"},
                        # Object field - profile.location
                        {"query": "profile.location.city:Jakarta", "label": "obj_profile_city"},
                        {"query": "profile.location.city:Surabaya", "label": "obj_profile_surabaya"},
                        {"query": "profile.location.country:Indonesia", "label": "obj_profile_country"},
                        # Deeply nested - metadata.metadata
                        {"query": "metadata.id:1", "label": "nested_md_id"},
                        {"query": "metadata.name:active", "label": "nested_md_name"},
                        {"query": "metadata.metadata.created:2025", "label": "nested_md_metadata_created"},
                        # metadata.metadata.tags[0] - array index access
                        {"query": "metadata.metadata.tags[0]:python", "label": "nested_md_tags0"},
                        {"query": "metadata.metadata.tags[0]:golang", "label": "nested_md_tags0_b"},
                        # metadata.metadata.tags - full array search
                        {"query": "metadata.metadata.tags:docker", "label": "nested_md_tags_full"},
                        # Text search on name
                        {"query": "name:alice", "label": "text_name_partial"},
                        {"query": "name:john", "label": "text_name_john"},
                        # Boolean
                        {"query": "metadata.active:true", "label": "bool_active_true"},
                        {"query": "metadata.active:false", "label": "bool_active_false"},
                        # AND / OR / NOT combinations
                        {"query": "profile.location.city:Jakarta AND metadata.active:true", "label": "and_city_active"},
                        {"query": "preferences.tags:python OR preferences.tags:golang", "label": "or_tags"},
                        {"query": "profile.location.city:Jakarta AND NOT metadata.active:false", "label": "not_combined"},
                        # Match none
                        {"query": "city:NonExistent", "label": "match_none"},
                    ]
                    # Aggregate: field + type for per-type breakdown
                    # Types: terms, date_histogram, histogram, stats, min, max, sum, cardinality
                    aggregate_aggs = [
                        {"field": "profile.location.city", "type": "terms", "label": "city"},
                        {"field": "profile.location.country", "type": "terms", "label": "country"},
                        {"field": "preferences.theme", "type": "terms", "label": "theme"},
                        {"field": "preferences.tags", "type": "terms", "label": "tags"},
                        {"field": "metadata.metadata.tags", "type": "terms", "label": "md_tags"},
                        {"field": "profile.age", "type": "stats", "label": "age_stats"},
                        {"field": "metadata.id", "type": "min", "label": "id_min"},
                        {"field": "metadata.id", "type": "max", "label": "id_max"},
                    ]
                    range_tests = [
                        {"field": "metadata.id", "lo": 1, "hi": 100},
                        {"field": "metadata.id", "lo": 50, "hi": 200},
                        {"field": "profile.age", "lo": 18, "hi": 40},
                        {"field": "profile.age", "lo": 40, "hi": 80},
                    ]
                    wildcard_tests = [
                        "Jakarta", "python", "golang", "docker",
                        "Alice", "alpha",
                    ]
                elif schema == "sosmed":
                    # Social media dataset: post_id, user_id, username, platform, content, timestamp, likes, shares, comments, impressions, followers
                    queries = [
                        {"query": "*", "label": "match_all"},
                        {"query": "platform:instagram", "label": "platform_instagram"},
                        {"query": "platform:facebook", "label": "platform_facebook"},
                        {"query": "platform:tiktok", "label": "platform_tiktok"},
                        {"query": "username:user_*", "label": "username_wildcard"},
                        {"query": "content:hiring", "label": "content_hiring"},
                        {"query": "content:AI", "label": "content_AI"},
                        {"query": "content:hot", "label": "content_hot"},
                        {"query": "likes:0", "label": "likes_zero"},
                        {"query": "shares:0", "label": "shares_zero"},
                        {"query": "comments:0", "label": "comments_zero"},
                        {"query": "impressions:1000", "label": "impressions_1k"},
                        {"query": "followers:1000", "label": "followers_1k"},
                        {"query": "platform:instagram AND likes:0", "label": "and_ig_no_likes"},
                        {"query": "platform:facebook OR platform:tiktok", "label": "or_platform"},
                        {"query": "likes:0 AND NOT platform:instagram", "label": "not_ig_zero_likes"},
                        {"query": "content:NonexistentWord", "label": "match_none"},
                    ]
                    aggregate_aggs = [
                        {"field": "platform", "type": "terms", "label": "platform"},
                        {"field": "username", "type": "terms", "label": "username"},
                        {"field": "likes", "type": "stats", "label": "likes_stats"},
                        {"field": "shares", "type": "stats", "label": "shares_stats"},
                        {"field": "comments", "type": "stats", "label": "comments_stats"},
                        {"field": "impressions", "type": "min", "label": "impressions_min"},
                        {"field": "impressions", "type": "max", "label": "impressions_max"},
                    ]
                    range_tests = [
                        {"field": "likes", "lo": 0, "hi": 100},
                        {"field": "likes", "lo": 100, "hi": 1000},
                        {"field": "shares", "lo": 0, "hi": 50},
                        {"field": "impressions", "lo": 0, "hi": 1000},
                    ]
                    wildcard_tests = ["user_", "instagram", "facebook", "hiring", "AI"]
                elif schema == "article":
                    # Article/blog dataset: id, title, content, tags, views, published_at, author
                    queries = [
                        {"query": "*", "label": "match_all"},
                        {"query": "author:user_*", "label": "author_wildcard"},
                        {"query": "tags:machine-learning", "label": "tags_ml"},
                        {"query": "tags:api", "label": "tags_api"},
                        {"query": "tags:devops", "label": "tags_devops"},
                        {"query": "tags:security", "label": "tags_security"},
                        {"query": "tags:performance", "label": "tags_perf"},
                        {"query": "content:performance", "label": "content_perf"},
                        {"query": "content:testing", "label": "content_testing"},
                        {"query": "content:scalability", "label": "content_scalability"},
                        {"query": "content:distributed", "label": "content_distributed"},
                        {"query": "content:monitoring", "label": "content_monitoring"},
                        {"query": "views:0", "label": "views_zero"},
                        {"query": "published_at:2025", "label": "pub_2025"},
                        {"query": "title:Microservices", "label": "title_microservices"},
                        {"query": "title:Kubernetes", "label": "title_k8s"},
                        {"query": "title:Performance", "label": "title_perf"},
                        {"query": "tags:machine-learning AND tags:api", "label": "and_tags"},
                        {"query": "title:microservices OR title:docker", "label": "or_title"},
                        {"query": "content:security AND NOT tags:devops", "label": "not_content"},
                        {"query": "content:NonexistentWordXYZ", "label": "match_none"},
                    ]
                    aggregate_aggs = [
                        {"field": "tags", "type": "terms", "label": "tags"},
                        {"field": "author", "type": "terms", "label": "author"},
                        {"field": "published_at", "type": "terms", "label": "pub_year"},
                        {"field": "views", "type": "stats", "label": "views_stats"},
                        {"field": "views", "type": "min", "label": "views_min"},
                        {"field": "views", "type": "max", "label": "views_max"},
                    ]
                    range_tests = [
                        {"field": "views", "lo": 0, "hi": 100},
                        {"field": "views", "lo": 100, "hi": 10000},
                        {"field": "views", "lo": 10000, "hi": 100000},
                        {"field": "id", "lo": 1, "hi": 10000},
                    ]
                    wildcard_tests = ["micro", "kube", "perform", "data", "cloud"]
                elif schema == "adsb":
                    # Aviation ADS-B: icao_address, aircraft_type, callsign, flight, origin/destination, altitude, speed, heading, lat/lon, timestamp, country, status
                    queries = [
                        {"query": "*", "label": "match_all"},
                        {"query": "status:active", "label": "status_active"},
                        {"query": "status:landed", "label": "status_landed"},
                        {"query": "origin:WIII", "label": "origin_wiii"},
                        {"query": "origin:WSSS", "label": "origin_wsss"},
                        {"query": "destination:OMDB", "label": "dest_omdb"},
                        {"query": "aircraft_type:A20N", "label": "ac_type_a20n"},
                        {"query": "aircraft_type:B738", "label": "ac_type_b738"},
                        {"query": "country:ID", "label": "country_id"},
                        {"query": "country:US", "label": "country_us"},
                        {"query": "altitude:40000", "label": "alt_40k"},
                        {"query": "speed:500", "label": "speed_500"},
                        {"query": "level:INFO", "label": "level_info"},
                        {"query": "level:ERROR", "label": "level_error"},
                        {"query": "origin:WIII AND status:active", "label": "and_origin_active"},
                        {"query": "origin:WSSS OR destination:WIII", "label": "or_route"},
                        {"query": "status:active AND NOT country:ID", "label": "not_id"},
                        {"query": "icao_address:XXXXXX", "label": "match_none"},
                    ]
                    aggregate_aggs = [
                        {"field": "status", "type": "terms", "label": "status"},
                        {"field": "country", "type": "terms", "label": "country"},
                        {"field": "origin", "type": "terms", "label": "origin"},
                        {"field": "aircraft_type", "type": "terms", "label": "ac_type"},
                        {"field": "altitude", "type": "stats", "label": "altitude_stats"},
                        {"field": "altitude", "type": "min", "label": "altitude_min"},
                        {"field": "altitude", "type": "max", "label": "altitude_max"},
                    ]
                    range_tests = [
                        {"field": "altitude", "lo": 28000, "hi": 35000},
                        {"field": "altitude", "lo": 35000, "hi": 41000},
                        {"field": "speed", "lo": 380, "hi": 500},
                        {"field": "speed", "lo": 500, "hi": 540},
                    ]
                    wildcard_tests = ["WIII", "WSSS", "A20N", "B738", "active"]
                elif schema == "campaign":
                    # AdTech DSP: campaign_id, advertiser, campaign, platform, country, status, bid, impressions, clicks, conversions, spend, ctr, cpc, roas, budget, timestamp, frequency
                    queries = [
                        {"query": "*", "label": "match_all"},
                        {"query": "status:active", "label": "status_active"},
                        {"query": "status:paused", "label": "status_paused"},
                        {"query": "platform:facebook", "label": "platform_facebook"},
                        {"query": "platform:instagram", "label": "platform_instagram"},
                        {"query": "platform:google", "label": "platform_google"},
                        {"query": "platform:tiktok", "label": "platform_tiktok"},
                        {"query": "country:ID", "label": "country_id"},
                        {"query": "country:US", "label": "country_us"},
                        {"query": "advertiser:ShopTokoID", "label": "adv_shop"},
                        {"query": "bid:1.00", "label": "bid_1"},
                        {"query": "roas:5.00", "label": "roas_5"},
                        {"query": "status:active AND platform:facebook", "label": "and_active_fb"},
                        {"query": "platform:instagram OR platform:tiktok", "label": "or_social"},
                        {"query": "status:active AND NOT country:ID", "label": "not_id"},
                        {"query": "campaign:nonexistent", "label": "match_none"},
                    ]
                    aggregate_aggs = [
                        {"field": "status", "type": "terms", "label": "status"},
                        {"field": "platform", "type": "terms", "label": "platform"},
                        {"field": "country", "type": "terms", "label": "country"},
                        {"field": "advertiser", "type": "terms", "label": "advertiser"},
                        {"field": "impressions", "type": "stats", "label": "impr_stats"},
                        {"field": "clicks", "type": "stats", "label": "clicks_stats"},
                        {"field": "bid", "type": "min", "label": "bid_min"},
                        {"field": "bid", "type": "max", "label": "bid_max"},
                    ]
                    range_tests = [
                        {"field": "impressions", "lo": 0, "hi": 10000},
                        {"field": "impressions", "lo": 10000, "hi": 100000},
                        {"field": "bid", "lo": 0.05, "hi": 2.00},
                        {"field": "bid", "lo": 2.00, "hi": 15.00},
                    ]
                    wildcard_tests = ["active", "facebook", "instagram", "Shop", "ID"]
                elif schema == "devops":
                    # DevOps/SRE: timestamp, level, service, region, message, trace_id, duration_ms, status_code, host, request_id
                    queries = [
                        {"query": "*", "label": "match_all"},
                        {"query": "level:INFO", "label": "level_info"},
                        {"query": "level:WARN", "label": "level_warn"},
                        {"query": "level:ERROR", "label": "level_error"},
                        {"query": "service:api-gateway", "label": "svc_api_gateway"},
                        {"query": "service:auth-service", "label": "svc_auth"},
                        {"query": "service:payment-service", "label": "svc_payment"},
                        {"query": "region:us-east-1", "label": "region_us"},
                        {"query": "region:ap-southeast-1", "label": "region_apse"},
                        {"query": "status_code:200", "label": "status_200"},
                        {"query": "status_code:500", "label": "status_500"},
                        {"query": "duration_ms:1000", "label": "dur_1k"},
                        {"query": "level:ERROR AND service:api-gateway", "label": "and_err_gateway"},
                        {"query": "level:WARN OR level:ERROR", "label": "or_warn_err"},
                        {"query": "level:INFO AND NOT region:us-east-1", "label": "not_region"},
                        {"query": "message:nonexistentword", "label": "match_none"},
                    ]
                    aggregate_aggs = [
                        {"field": "level", "type": "terms", "label": "level"},
                        {"field": "service", "type": "terms", "label": "service"},
                        {"field": "region", "type": "terms", "label": "region"},
                        {"field": "status_code", "type": "terms", "label": "status_code"},
                        {"field": "duration_ms", "type": "stats", "label": "dur_stats"},
                        {"field": "duration_ms", "type": "min", "label": "dur_min"},
                        {"field": "duration_ms", "type": "max", "label": "dur_max"},
                    ]
                    range_tests = [
                        {"field": "duration_ms", "lo": 0, "hi": 100},
                        {"field": "duration_ms", "lo": 100, "hi": 1000},
                        {"field": "duration_ms", "lo": 1000, "hi": 5000},
                    ]
                    wildcard_tests = ["api-gateway", "auth-service", "ERROR", "us-east", "WARN"]
                elif schema == "blockchain":
                    # Solana blockchain: signature, slot, timestamp, fee, status, signer, num_accounts, compute_units, instructions, has_error_log, programs, first_program, first_instruction_data
                    queries = [
                        {"query": "*", "label": "match_all"},
                        {"query": "status:success", "label": "status_success"},
                        {"query": "status:failed", "label": "status_failed"},
                        {"query": "first_program:raydium", "label": "prog_raydium"},
                        {"query": "first_program:jupiter", "label": "prog_jupiter"},
                        {"query": "first_program:orca", "label": "prog_orca"},
                        {"query": "instructions:1", "label": "instr_1"},
                        {"query": "has_error_log:true", "label": "err_log_true"},
                        {"query": "fee:5000", "label": "fee_5k"},
                        {"query": "compute_units:200000", "label": "cu_200k"},
                        {"query": "status:success AND first_program:raydium", "label": "and_success_ray"},
                        {"query": "first_program:raydium OR first_program:jupiter", "label": "or_dex"},
                        {"query": "status:failed AND NOT first_program:raydium", "label": "not_raydium"},
                        {"query": "signature:XXXXXX", "label": "match_none"},
                    ]
                    aggregate_aggs = [
                        {"field": "status", "type": "terms", "label": "status"},
                        {"field": "first_program", "type": "terms", "label": "first_program"},
                        {"field": "first_instruction_data", "type": "terms", "label": "first_instruction"},
                        {"field": "fee", "type": "stats", "label": "fee_stats"},
                        {"field": "compute_units", "type": "stats", "label": "cu_stats"},
                        {"field": "compute_units", "type": "min", "label": "cu_min"},
                        {"field": "compute_units", "type": "max", "label": "cu_max"},
                    ]
                    range_tests = [
                        {"field": "fee", "lo": 5000, "hi": 10000},
                        {"field": "compute_units", "lo": 120000, "hi": 200000},
                        {"field": "num_accounts", "lo": 3, "hi": 8},
                        {"field": "num_accounts", "lo": 8, "hi": 15},
                    ]
                    wildcard_tests = ["raydium", "jupiter", "success", "swap", "initialize"]
                else:
                    queries = [
                        # Match-all
                        {"query": "*", "label": "match_all"},
                        # Keyword exact
                        {"query": "city:Jakarta", "label": "kw_city_jakarta"},
                        {"query": "city:Surabaya", "label": "kw_city_surabaya"},
                        {"query": "status:active", "label": "kw_status_active"},
                        {"query": "status:inactive", "label": "kw_status_inactive"},
                        {"query": "country:Indonesia", "label": "kw_country"},
                        # Text partial
                        {"query": "name:alice", "label": "text_name"},
                        {"query": "email:dev.io", "label": "text_email"},
                        # Array tags
                        {"query": "tags:python", "label": "array_tags_python"},
                        {"query": "tags:golang", "label": "array_tags_golang"},
                        {"query": "tags:docker", "label": "array_tags_docker"},
                        # Boolean
                        {"query": "is_verified:true", "label": "bool_verified"},
                        {"query": "is_verified:false", "label": "bool_unverified"},
                        # AND / OR / NOT
                        {"query": "city:Jakarta AND status:active", "label": "and_city_status"},
                        {"query": "status:active OR status:pending", "label": "or_status"},
                        {"query": "city:Jakarta AND NOT status:inactive", "label": "not_status"},
                        # Match none
                        {"query": "city:NonExistent", "label": "match_none"},
                    ]
                    # Standard schema aggregations
                    aggregate_aggs = [
                        {"field": "city", "type": "terms", "label": "city"},
                        {"field": "country", "type": "terms", "label": "country"},
                        {"field": "status", "type": "terms", "label": "status"},
                        {"field": "tags", "type": "terms", "label": "tags"},
                        {"field": "balance", "type": "stats", "label": "balance_stats"},
                        {"field": "balance", "type": "min", "label": "balance_min"},
                        {"field": "balance", "type": "max", "label": "balance_max"},
                    ]
                    range_tests = [
                        {"field": "balance", "lo": 100000, "hi": 500000},
                        {"field": "balance", "lo": 500000, "hi": 1000000},
                    ]
                    wildcard_tests = ["Jakarta", "active", "dev", "python", "Alice"]

                suite.run_benchmark(
                    engine_name=engine_name,
                    data_path=data_path,
                    config=config,
                    queries=queries,
                    aggregate_aggs=aggregate_aggs,
                    range_tests=range_tests,
                    wildcard_tests=wildcard_tests,
                    iterations=10,
                    workers=workers,
                )

    suite.generate_report()
    suite.print_summary()


def main():
    parser = argparse.ArgumentParser(description="Flatbench - Search Engine Benchmark Suite")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Generate command
    gen_parser = subparsers.add_parser("generate", help="Generate benchmark dataset")
    gen_parser.add_argument("--schema", "-s", default="standard",
                           choices=list(SCHEMAS.keys()))
    gen_parser.add_argument("--rows", "-r", type=int, default=100000)
    gen_parser.add_argument("--output", "-o", required=True)
    gen_parser.add_argument("--format", "-f", default="csv", choices=["csv", "jsonl"])

    # Run command
    run_parser = subparsers.add_parser("run", help="Run benchmark on a dataset")
    run_parser.add_argument("--engine", "-e", required=True,
                          choices=list_engines())
    run_parser.add_argument("--data", "-d", required=True, help="Data file path")
    run_parser.add_argument("--index-dir", "-i", required=True, help="Index directory")
    run_parser.add_argument("--output", "-o", default="./output", help="Output directory")
    run_parser.add_argument("--iterations", default=10, type=int)

    # Compare command
    compare_parser = subparsers.add_parser("compare", help="Compare multiple engines")
    compare_parser.add_argument("--engines", "-e", required=True,
                               help="Comma-separated engine names")
    compare_parser.add_argument("--sizes", "-s", required=True, nargs="+", type=int,
                               help="Dataset sizes to test")
    compare_parser.add_argument("--schema", default="standard",
                               choices=list(SCHEMAS.keys()))
    compare_parser.add_argument("--workers", "-w", default=1, type=int,
                               help="Number of parallel workers for flatseek indexing (default: 1)")
    compare_parser.add_argument("--format", "-f", default="csv",
                               choices=["csv", "jsonl"],
                               help="Data format (default: csv)")
    compare_parser.add_argument("--source", "-S", default="",
                               help="Source file or directory path (if set, uses this instead of generating)")
    compare_parser.add_argument("--mode", "-m", default="normal",
                               choices=["normal", "tmpfs"],
                               help="Index storage mode: normal (disk) or tmpfs (memory-backed)")

    args = parser.parse_args()

    if args.command == "generate":
        generate_dataset(args.schema, args.rows, args.output, args.format)

    elif args.command == "compare":
        engines = args.engines.split(",")
        run_compare(engines, args.sizes, args.schema, workers=args.workers,
                    source_format=args.format, source_path=args.source, mode=args.mode)

    elif args.command == "run":
        from runners import flatseek, sqlite, elasticsearch  # noqa
        suite = BenchmarkSuite(args.output)
        config = EngineConfig(name=args.engine, data_dir=args.index_dir)
        runner_class = get_engine(args.engine)
        runner = runner_class(config)

        print(f"Building index from {args.data}...")
        build_result = runner.build_index(args.data)
        print(f"Index built in {build_result.duration_ms:.2f}ms")

        # Run some default tests
        result = runner.search("Jakarta", iterations=args.iterations)
        print(f"Search 'Jakarta': p50={result.latency_p50_ms:.3f}ms, ops/s={result.ops_per_sec:.2f}")

        result = runner.search("*", iterations=args.iterations)
        print(f"Search '*': p50={result.latency_p50_ms:.3f}ms, ops/s={result.ops_per_sec:.2f}")

        if runner.supports_aggregate:
            result = runner.aggregate("city")
            print(f"Aggregate 'city': {result.rows} buckets in {result.duration_ms:.2f}ms")

        suite.generate_report()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()