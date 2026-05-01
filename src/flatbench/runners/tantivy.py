"""Tantivy benchmark runner."""

import os
import time
import json
import csv
import shutil
from pathlib import Path

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


@register_engine("tantivy")
class TantivyRunner(BaseRunner):
    """Benchmark runner for Tantivy (Rust FTS)."""

    name = "tantivy"
    supports_aggregate = True
    supports_range_query = True
    supports_wildcard = True

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self.index_path = os.path.join(config.data_dir, "tantivy_index")
        self._index = None

    def _build_schema(self):
        """Build Tantivy schema."""
        import tantivy
        schema_builder = tantivy.SchemaBuilder()
        schema_builder.add_text_field("id", stored=True)
        schema_builder.add_text_field("title", stored=True)
        schema_builder.add_text_field("content", stored=True)
        schema_builder.add_text_field("tags", stored=True)
        schema_builder.add_unsigned_field("views", stored=True, indexed=True)
        schema_builder.add_text_field("published_at", stored=True)
        schema_builder.add_text_field("author", stored=True)
        return schema_builder.build()

    def _get_index(self):
        """Get or create Tantivy index."""
        import tantivy

        if self._index is None:
            if os.path.exists(self.index_path):
                shutil.rmtree(self.index_path)

            os.makedirs(self.index_path, exist_ok=True)
            schema = self._build_schema()
            self._index = tantivy.Index(schema)
        else:
            # Reload to see latest commits
            self._index.reload()
        return self._index

    def build_index(self, data_path: str, workers: int = 1) -> BenchmarkResult:
        """Build index from CSV data using Tantivy."""
        import tantivy

        # Clean up existing index
        if os.path.exists(self.index_path):
            shutil.rmtree(self.index_path)
        os.makedirs(self.index_path, exist_ok=True)

        schema = self._build_schema()
        index = tantivy.Index(schema)

        start_time = time.perf_counter()
        rows_indexed = 0

        try:
            writer = index.writer()
            with open(data_path, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    flat_row = _parse_row(row)

                    doc = tantivy.Document()
                    for key, val in flat_row.items():
                        if key == "views" and isinstance(val, (int, float)):
                            doc.add_unsigned(key, int(val))
                        elif isinstance(val, (int, float)):
                            doc.add_text(key, str(val))
                        else:
                            doc.add_text(key, str(val) if val else "")

                    writer.add_document(doc)
                    rows_indexed += 1

                    if rows_indexed % 50000 == 0:
                        print(f"  Tantivy: {rows_indexed:,} docs indexed...")

            writer.commit()
            writer = None  # Release writer lock
        except Exception as e:
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(data_path),
                operation="build_index",
                rows=rows_indexed,
                duration_ms=(time.perf_counter() - start_time) * 1000,
                ops_per_sec=0,
                error=str(e)
            )

        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = rows_indexed / (duration_ms / 1000) if duration_ms > 0 else 0

        # Get index size
        index_size = 0
        if os.path.exists(self.index_path):
            index_size = sum(
                os.path.getsize(os.path.join(dirpath, f))
                for dirpath, _, files in os.walk(self.index_path)
                for f in files
            ) / 1024 / 1024

        self._index = index

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
            memory_mb=index_size,
            metadata={"index_size_mb": index_size}
        )

    def search(self, query: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run search queries using Tantivy."""
        import tantivy

        latencies = []
        result_counts = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                index = self._get_index()
                q = index.parse_query(query, ["title", "content"])
                # Reload index to see latest commits
                index.reload()
                searcher = index.searcher()
                results = searcher.search(q, limit=1000)
                result_counts.append(len(results.hits))
            except Exception as e:
                result_counts.append(0)
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
            metadata={"query": query, "result_count": result_counts[-1] if result_counts else 0}
        )

    def aggregate(self, field: str, query: str = "*", agg_type: str = "terms", **kwargs) -> BenchmarkResult:
        """Run aggregation queries using Tantivy."""
        import tantivy
        from collections import Counter

        start_time = time.perf_counter()

        try:
            index = self._get_index()
            searcher = index.searcher()

            if query != "*":
                q = index.parse_query(query, ["title", "content"])
                results = searcher.search(q, limit=10000)
            else:
                q = index.parse_query("*", ["title"])
                results = searcher.search(q, limit=10000)

            if agg_type in ("terms", "cardinality"):
                values = []
                for hit_score, doc_addr in results.hits:
                    try:
                        retrieved_doc = searcher.doc(doc_addr)
                        field_val = retrieved_doc.get_first(field)
                        if field_val:
                            val = str(field_val[0]) if isinstance(field_val, list) else str(field_val)
                            values.append(val)
                    except:
                        pass
                counter = Counter(values)
                rows = len(counter)
            elif agg_type in ("min", "max", "sum", "avg", "stats"):
                num_values = []
                for hit_score, doc_addr in results.hits:
                    try:
                        retrieved_doc = searcher.doc(doc_addr)
                        field_val = retrieved_doc.get_first(field)
                        if field_val:
                            val = field_val[0] if isinstance(field_val, list) else field_val
                            num_values.append(float(val))
                    except:
                        pass
                if not num_values:
                    rows = 0
                else:
                    rows = 1
            else:
                rows = 0
        except Exception as e:
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(self.config.data_dir),
                operation="aggregate",
                rows=0,
                duration_ms=(time.perf_counter() - start_time) * 1000,
                ops_per_sec=0,
                error=str(e)
            )

        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0

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
        """Range query on numeric columns."""
        import tantivy

        start_time = time.perf_counter()

        try:
            index = self._get_index()
            schema = index.schema

            # Determine field type for range query
            field_type = tantivy.FieldType.Unsigned
            q = tantivy.Query.range_query(schema, field, field_type, lo, hi)
            searcher = index.searcher()
            results = searcher.search(q, limit=1000)
            rows = len(results.hits)
        except Exception as e:
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(self.config.data_dir),
                operation="range_query",
                rows=0,
                duration_ms=(time.perf_counter() - start_time) * 1000,
                ops_per_sec=0,
                error=str(e)
            )

        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = 1000 / duration_ms if duration_ms > 0 else 0

        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(self.config.data_dir),
            operation="range_query",
            rows=rows,
            duration_ms=duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=duration_ms,
            latency_p95_ms=duration_ms,
            latency_p99_ms=duration_ms,
            metadata={"field": field, "lo": lo, "hi": hi, "result_count": rows}
        )

    def wildcard_search(self, pattern: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Wildcard search using Tantivy."""
        import tantivy

        latencies = []
        start_time = time.perf_counter()

        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                index = self._get_index()
                q = index.parse_query(f"*{pattern}*", ["title", "content"])
                searcher = index.searcher()
                searcher.search(q, limit=1000)
            except:
                pass
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
        """Clean up index directory."""
        if hasattr(self, 'index_path') and os.path.exists(self.index_path):
            shutil.rmtree(self.index_path)
        self._index = None