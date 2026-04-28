"""SQLite FTS5 benchmark runner."""

import os
import time
import sqlite3
from pathlib import Path

from . import BaseRunner, BenchmarkResult, EngineConfig, register_engine


@register_engine("sqlite")
class SqliteRunner(BaseRunner):
    """Benchmark runner for SQLite FTS5."""

    name = "sqlite"
    supports_aggregate = False  # SQLite FTS5 doesn't support aggregations well
    supports_range_query = True
    supports_wildcard = True

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self._conn = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            db_path = os.path.join(self.config.data_dir, "sqlitefts.db")
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
        return self._conn

    def build_index(self, data_path: str) -> BenchmarkResult:
        """Build FTS5 index from CSV data."""
        import csv

        db_path = os.path.join(self.config.data_dir, "sqlitefts.db")
        os.makedirs(self.config.data_dir, exist_ok=True)

        # Remove existing db
        if os.path.exists(db_path):
            os.remove(db_path)

        conn = sqlite3.connect(db_path)
        try:
            conn.enable_load_extension(True)
        except AttributeError:
            pass  # Some SQLite builds don't have load extension

        # Try to load fts5 extension
        try:
            conn.execute("CREATE VIRTUAL TABLE documents USING fts5(id, name, email, phone, city, country, status, balance, created_at, updated_at, is_verified, tags)")
        except Exception as e:
            conn.close()
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(data_path),
                operation="build_index",
                rows=0,
                duration_ms=0,
                ops_per_sec=0,
                error=f"FTS5 not available: {e}"
            )

        start_time = time.perf_counter()
        rows_imported = 0

        try:
            with open(data_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames

                # Recreate table with actual columns
                conn.execute("DROP TABLE IF EXISTS documents")
                conn.execute("DROP TABLE IF EXISTS idx")
                conn.execute(f"CREATE VIRTUAL TABLE idx USING fts5({', '.join(headers)})")

                for row in reader:
                    # Convert tags list to string if needed
                    if 'tags' in row and row['tags']:
                        import json
                        try:
                            tags_list = json.loads(row['tags'])
                            row['tags'] = ' '.join(tags_list)
                        except:
                            pass
                    conn.execute(f"INSERT INTO idx VALUES ({', '.join(['?' for _ in headers])})",
                                [row.get(h, '') for h in headers])
                    rows_imported += 1

                    if rows_imported % 50000 == 0:
                        print(f"  SQLite: {rows_imported:,} rows imported...")

            conn.commit()
        except Exception as e:
            conn.close()
            return BenchmarkResult(
                engine=self.name,
                dataset=os.path.basename(data_path),
                operation="build_index",
                rows=rows_imported,
                duration_ms=(time.perf_counter() - start_time) * 1000,
                ops_per_sec=0,
                error=str(e)
            )

        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        ops_per_sec = rows_imported / (duration_ms / 1000) if duration_ms > 0 else 0

        # Get index size
        index_size = os.path.getsize(db_path) / 1024 / 1024

        conn.close()
        self._conn = None

        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(data_path),
            operation="build_index",
            rows=rows_imported,
            duration_ms=duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=duration_ms,
            latency_p95_ms=duration_ms,
            latency_p99_ms=duration_ms,
            memory_mb=index_size,
            metadata={
                "index_size_mb": index_size,
                "columns_count": len(headers) if rows_imported > 0 else 0,
            }
        )

    def search(self, query: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Run search queries using FTS5 MATCH."""
        if self._conn is None:
            db_path = os.path.join(self.config.data_dir, "sqlitefts.db")
            if not os.path.exists(db_path):
                return BenchmarkResult(
                    engine=self.name,
                    dataset="",
                    operation="search",
                    rows=0,
                    duration_ms=0,
                    ops_per_sec=0,
                    error="Database not found. Run build_index first."
                )
            self._conn = sqlite3.connect(db_path, check_same_thread=False)

        latencies = []
        result_counts = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                cursor = self._conn.execute(
                    "SELECT id FROM idx WHERE idx MATCH ? LIMIT 1000",
                    (query,)
                )
                results = cursor.fetchall()
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
            metadata={
                "query": query,
                "result_count": result_counts[-1] if result_counts else 0
            }
        )

    def aggregate(self, field: str, query: str = "*", **kwargs) -> BenchmarkResult:
        """SQLite FTS5 doesn't support aggregations."""
        return BenchmarkResult(
            engine=self.name,
            dataset=os.path.basename(self.config.data_dir),
            operation="aggregate",
            rows=0,
            duration_ms=0,
            ops_per_sec=0,
            error="SQLite FTS5 does not support aggregations"
        )

    def range_query(self, field: str, lo: int, hi: int, **kwargs) -> BenchmarkResult:
        """Range query on numeric columns."""
        if self._conn is None:
            db_path = os.path.join(self.config.data_dir, "sqlitefts.db")
            if not os.path.exists(db_path):
                return BenchmarkResult(
                    engine=self.name,
                    dataset="",
                    operation="range_query",
                    rows=0,
                    duration_ms=0,
                    ops_per_sec=0,
                    error="Database not found. Run build_index first."
                )
            self._conn = sqlite3.connect(db_path, check_same_thread=False)

        start_time = time.perf_counter()
        try:
            cursor = self._conn.execute(
                f"SELECT id FROM idx WHERE {field} BETWEEN ? AND ? LIMIT 1000",
                (lo, hi)
            )
            results = cursor.fetchall()
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
            rows=len(results),
            duration_ms=duration_ms,
            ops_per_sec=ops_per_sec,
            latency_p50_ms=duration_ms,
            latency_p95_ms=duration_ms,
            latency_p99_ms=duration_ms,
            metadata={"field": field, "lo": lo, "hi": hi, "result_count": len(results)}
        )

    def wildcard_search(self, pattern: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        """Wildcard search using FTS5 prefix matching."""
        if self._conn is None:
            db_path = os.path.join(self.config.data_dir, "sqlitefts.db")
            if not os.path.exists(db_path):
                return BenchmarkResult(
                    engine=self.name,
                    dataset="",
                    operation="wildcard_search",
                    rows=0,
                    duration_ms=0,
                    ops_per_sec=0,
                    error="Database not found. Run build_index first."
                )
            self._conn = sqlite3.connect(db_path, check_same_thread=False)

        # Convert *pattern* to FTS5 prefix syntax
        fts_query = f"\"{pattern}\"*"

        latencies = []
        start_time = time.perf_counter()

        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                cursor = self._conn.execute(
                    "SELECT id FROM idx WHERE idx MATCH ? LIMIT 1000",
                    (fts_query,)
                )
                results = cursor.fetchall()
            except:
                results = []
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
            metadata={"pattern": pattern, "fts_query": fts_query}
        )

    def cleanup(self):
        """Clean up resources."""
        if self._conn:
            self._conn.close()
            self._conn = None