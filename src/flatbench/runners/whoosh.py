"""Whoosh benchmark runner."""

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


@register_engine("whoosh")
class WhooshRunner(BaseRunner):
    """Benchmark runner for Whoosh (pure Python FTS)."""

    name = "whoosh"
    supports_aggregate = True
    supports_range_query = True
    supports_wildcard = True

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self.index_path = os.path.join(config.data_dir, "whoosh_index")
        self._writer = None

    def _get_writer(self):
        if self._writer is None:
            from whoosh.index import create_in, open_dir
            from whoosh.fields import Schema, TEXT, ID, NUMERIC, KEYWORD, STORED
            from whoosh.analysis import StemmingAnalyzer

            schema = Schema(
                id=ID(stored=True),
                title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
                content=TEXT(stored=True, analyzer=StemmingAnalyzer()),
                tags=KEYWORD(stored=True, commas=True),
                views=NUMERIC(stored=True, numtype=int),
                published_at=STORED,
                author=KEYWORD(stored=True, commas=True),
            )

            if os.path.exists(self.index_path):
                shutil.rmtree(self.index_path)

            os.makedirs(self.index_path, exist_ok=True)
            ix = create_in(self.index_path, schema)
            self._writer = ix.writer()
        return self._writer

    def build_index(self, data_path: str, workers: int = 1) -> BenchmarkResult:
        """Build index from CSV data using Whoosh."""
        from whoosh.index import create_in
        from whoosh.fields import Schema, TEXT, ID, NUMERIC, KEYWORD, STORED
        from whoosh.analysis import StemmingAnalyzer

        # Clean up existing index
        if os.path.exists(self.index_path):
            shutil.rmtree(self.index_path)
        os.makedirs(self.index_path, exist_ok=True)

        schema = Schema(
            id=ID(stored=True),
            title=TEXT(stored=True, analyzer=StemmingAnalyzer()),
            content=TEXT(stored=True, analyzer=StemmingAnalyzer()),
            tags=KEYWORD(stored=True, commas=True),
            views=NUMERIC(stored=True, numtype=int),
            published_at=STORED,
            author=KEYWORD(stored=True, commas=True),
        )

        ix = create_in(self.index_path, schema)
        writer = ix.writer()

        start_time = time.perf_counter()
        rows_indexed = 0

        try:
            with open(data_path, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    flat_row = _parse_row(row)
                    doc = {}
                    for key, val in flat_row.items():
                        if key == "views" and isinstance(val, (int, float)):
                            doc[key] = val
                        elif isinstance(val, (int, float)):
                            doc[key] = str(val)
                        else:
                            doc[key] = val if val else ""

                    writer.add_document(**doc)
                    rows_indexed += 1

                    if rows_indexed % 50000 == 0:
                        print(f"  Whoosh: {rows_indexed:,} docs indexed...")
                        writer.commit()
                        writer = ix.writer()

            writer.commit()
        except Exception as e:
            writer.cancel()
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
        index_size = sum(
            os.path.getsize(os.path.join(dirpath, f))
            for dirpath, _, files in os.walk(self.index_path)
            for f in files
        ) / 1024 / 1024

        self._writer = None  # Reset writer

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
        """Run search queries using Whoosh."""
        from whoosh.index import open_dir
        from whoosh.qparser import MultifieldParser, OrGroup

        latencies = []
        result_counts = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                ix = open_dir(self.index_path)
                with ix.searcher() as searcher:
                    parser = MultifieldParser(["title", "content"], ix.schema, group=OrGroup)
                    q = parser.parse(query)
                    results = searcher.search(q, limit=1000)
                    result_counts.append(len(results))
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
        """Run aggregation queries using Whoosh."""
        from whoosh.index import open_dir
        from whoosh.qparser import MultifieldParser, OrGroup

        start_time = time.perf_counter()

        try:
            ix = open_dir(self.index_path)
            with ix.searcher() as searcher:
                if query != "*":
                    parser = MultifieldParser(["title", "content"], ix.schema, group=OrGroup)
                    q = parser.parse(query)
                    results = searcher.search(q, limit=10000)
                else:
                    # Match all documents using Every without field restrictions
                    from whoosh.query import Every
                    q = Every()
                    results = searcher.search(q, limit=10000)

                if agg_type in ("terms", "cardinality"):
                    # Count field values from Whoosh results
                    from collections import Counter
                    values = []
                    for hit in results:
                        # Use hit[field] or hit.get(field) for stored fields
                        try:
                            val = hit[field]
                            if val is not None:
                                values.append(val)
                        except (KeyError, AttributeError):
                            pass
                    counter = Counter(values)
                    rows = len(counter)
                elif agg_type in ("min", "max", "sum", "avg", "stats"):
                    values = [float(hit[field]) for hit in results if field in hit and hit[field]]
                    if not values:
                        rows = 0
                    elif agg_type == "min":
                        rows = 1
                    elif agg_type == "max":
                        rows = 1
                    elif agg_type == "sum":
                        rows = 1
                    elif agg_type == "avg":
                        rows = 1
                    elif agg_type == "stats":
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
        from whoosh.index import open_dir
        from whoosh.query import And, NumericRange

        start_time = time.perf_counter()

        try:
            ix = open_dir(self.index_path)
            with ix.searcher() as searcher:
                q = NumericRange(field, lo, hi)
                results = searcher.search(q, limit=1000)
                rows = len(results)
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
        """Wildcard search using Whoosh."""
        from whoosh.index import open_dir
        from whoosh.qparser import MultifieldParser, OrGroup

        latencies = []
        start_time = time.perf_counter()

        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                ix = open_dir(self.index_path)
                with ix.searcher() as searcher:
                    parser = MultifieldParser(["title", "content"], ix.schema, group=OrGroup)
                    q = parser.parse(f"*{pattern}*")
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
        self._writer = None