"""DuckDB benchmark runner."""

import os
import time
import json
import csv
from pathlib import Path

from . import BaseRunner, BenchmarkResult, EngineConfig, register_engine


@register_engine("duckdb")
class DuckDBRunner(BaseRunner):
    """Benchmark runner for DuckDB with basic text search (LIKE/FTS)."""

    name = "duckdb"
    supports_aggregate = True
    supports_range_query = True
    supports_wildcard = False  # DuckDB LIKE-based search doesn't have great wildcard support

    def __init__(self, config: EngineConfig):
        super().__init__(config)
        self._conn = None

    def _get_conn(self):
        if self._conn is None:
            import duckdb
            db_path = os.path.join(self.config.data_dir, "duckdb_fts.db")
            self._conn = duckdb.connect(db_path)
        return self._conn

    def build_index(self, data_path: str, workers: int = 1) -> BenchmarkResult:
        """Build index from CSV data using DuckDB."""
        import duckdb
        import json

        db_path = os.path.join(self.config.data_dir, "duckdb_fts.db")
        os.makedirs(self.config.data_dir, exist_ok=True)

        # Remove existing db
        if os.path.exists(db_path):
            os.remove(db_path)

        conn = duckdb.connect(db_path)
        start_time = time.perf_counter()
        rows_imported = 0

        try:
            # Read CSV and create table
            headers = None
            with open(data_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames

                # Create table with appropriate columns
                col_defs = []
                for h in headers:
                    col_defs.append(f'"{h}" VARCHAR')
                conn.execute(f"CREATE TABLE documents ({', '.join(col_defs)})")

                # Insert data
                for row in reader:
                    # Parse JSON strings
                    for k, v in row.items():
                        if v and isinstance(v, str):
                            try:
                                parsed = json.loads(v)
                                if isinstance(parsed, (list, dict)):
                                    row[k] = json.dumps(parsed)
                            except (json.JSONDecodeError, TypeError):
                                pass
                    vals = [row.get(h, '') for h in headers]
                    placeholders = ', '.join(['?' for _ in headers])
                    conn.execute(f"INSERT INTO documents VALUES ({placeholders})", vals)
                    rows_imported += 1

                    if rows_imported % 50000 == 0:
                        print(f"  DuckDB: {rows_imported:,} rows imported...")

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
        """Run search queries using DuckDB LIKE/ILIKE."""
        conn = self._get_conn()

        # Convert flatseek query to DuckDB LIKE query
        where_clause = self._flatseek_to_duckdb_where(query)

        latencies = []
        result_counts = []

        start_time = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                sql = f"SELECT * FROM documents WHERE {where_clause} LIMIT 1000"
                result = conn.execute(sql).fetchall()
                result_counts.append(len(result))
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

    def _flatseek_to_duckdb_where(self, query: str) -> str:
        """Convert flatseek-style query to DuckDB WHERE clause."""
        query = query.strip()

        if query == "*":
            return "1=1"

        if ":" in query:
            parts = query.split(":", 1)
            field = parts[0]
            value = parts[1].strip()
            # Remove wildcards for LIKE
            value = value.replace("*", "%").replace("?", "_")
            return f'"{field}" ILIKE \'%{value}%\''

        # Bare term - search all text columns
        query = query.replace("*", "%").replace("?", "_")
        return f'(name ILIKE \'%{query}%\' OR title ILIKE \'%{query}%\' OR content ILIKE \'%{query}%\' OR email ILIKE \'%{query}%\' OR city ILIKE \'%{query}%\' OR author ILIKE \'%{query}%\' OR tags ILIKE \'%{query}%\')'

    def aggregate(self, field: str, query: str = "*", agg_type: str = "terms", **kwargs) -> BenchmarkResult:
        """Run aggregation queries using DuckDB."""
        conn = self._get_conn()

        where_clause = self._flatseek_to_duckdb_where(query) if query != "*" else "1=1"

        start_time = time.perf_counter()
        try:
            if agg_type == "terms":
                result = conn.execute(
                    f'SELECT "{field}", COUNT(*) as cnt FROM documents '
                    f'WHERE {where_clause} GROUP BY "{field}" ORDER BY cnt DESC LIMIT 100'
                ).fetchall()
                rows = len(result)
            elif agg_type == "min":
                result = conn.execute(
                    f'SELECT MIN(TRY_CAST("{field}" AS DOUBLE)) FROM documents WHERE {where_clause}'
                ).fetchone()
                rows = 1
            elif agg_type == "max":
                result = conn.execute(
                    f'SELECT MAX(TRY_CAST("{field}" AS DOUBLE)) FROM documents WHERE {where_clause}'
                ).fetchone()
                rows = 1
            elif agg_type == "sum":
                result = conn.execute(
                    f'SELECT SUM(TRY_CAST("{field}" AS DOUBLE)) FROM documents WHERE {where_clause}'
                ).fetchone()
                rows = 1
            elif agg_type == "avg":
                result = conn.execute(
                    f'SELECT AVG(TRY_CAST("{field}" AS DOUBLE)) FROM documents WHERE {where_clause}'
                ).fetchone()
                rows = 1
            elif agg_type == "stats":
                result = conn.execute(
                    f'SELECT MIN(TRY_CAST("{field}" AS DOUBLE)), MAX(TRY_CAST("{field}" AS DOUBLE)), '
                    f'AVG(TRY_CAST("{field}" AS DOUBLE)), STDDEV_SAMP(TRY_CAST("{field}" AS DOUBLE)) '
                    f'FROM documents WHERE {where_clause}'
                ).fetchone()
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
        conn = self._get_conn()

        start_time = time.perf_counter()
        try:
            # Try numeric comparison first, fall back to string comparison
            try:
                result = conn.execute(
                    f'SELECT * FROM documents WHERE TRY_CAST("{field}" AS BIGINT) BETWEEN ? AND ? LIMIT 1000',
                    (lo, hi)
                ).fetchall()
            except Exception:
                result = conn.execute(
                    f'SELECT * FROM documents WHERE "{field}" BETWEEN ? AND ? LIMIT 1000',
                    (str(lo), str(hi))
                ).fetchall()
            rows = len(result)
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
        """Wildcard search using DuckDB LIKE."""
        conn = self._get_conn()

        latencies = []
        start_time = time.perf_counter()

        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                sql = f"SELECT * FROM documents WHERE name ILIKE \'%{pattern}%\' OR title ILIKE \'%{pattern}%\' OR content ILIKE \'%{pattern}%\' LIMIT 1000"
                conn.execute(sql).fetchall()
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
        """Clean up resources."""
        if self._conn:
            self._conn.close()
            self._conn = None
