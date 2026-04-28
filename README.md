<div align="center">

<img src="logo.svg" alt="Flabench" width="64" height="64">

# Flatbench

**Search engine benchmark suite — compare indexing, search, aggregation, and range query performance between Flatseek and Elasticsearch.**

[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

**Engine:** [flatseek](https://github.com/flatseek/flatseek)
&nbsp;&middot;&nbsp;
**Dashboard:** [flatlens](https://github.com/flatseek/flatlens)
&nbsp;&middot;&nbsp;
**Docs:** [flatseek.io/docs](https://flatseek.io/docs)

</div>

---

## Install

```
pip install flatseek
```

---

## Quick Start

```bash
# Start Elasticsearch (required)
make up

# Generate a dataset
python -m flatbench generate --schema article --rows 100000 --output ./data/test.csv

# Compare flatseek vs elasticsearch on 100K rows
python -m flatbench compare --engines flatseek,elasticsearch --sizes 100000 --schema article
```

---

## CLI Reference

### Commands

```bash
# Generate a synthetic dataset
python -m flatbench generate --schema <schema> --rows <N> --output <path> [--format csv|jsonl]

# Run benchmark on a single engine
python -m flatbench run --engine <name> --data <csv> --index-dir <dir> [--iterations N]

# Compare multiple engines across dataset sizes
python -m flatbench compare --engines <engines> --sizes <sizes> [--schema <schema>]
```

### Arguments

| Flag | Description | Default |
|------|-------------|---------|
| `--schema` | Data schema (`article`, `standard`, `ecommerce`, `logs`, `nested`, `sosmed`, `devops`, etc.) | `standard` |
| `--rows` | Number of rows to generate | 1000 |
| `--output`, `-o` | Output file path | Required |
| `--format` | Output format: `csv` or `jsonl` | `csv` |
| `--engines` | Comma-separated engine names | Required |
| `--sizes` | Dataset sizes to benchmark (rows) | Required |
| `--workers` | Parallel workers for flatseek indexing | `1` |
| `--iterations` | Query iterations for p50/p95/p99 | `10` |
| `--source` | Use existing CSV/JSONL as data source | Generated |
| `--mode` | Storage mode: `normal` (disk) or `tmpfs` (memory) | `normal` |

### Examples

```bash
# Generate article dataset (100K rows)
python -m flatbench generate --schema article --rows 100000 -o ./data/article.csv

# Run flatseek-only benchmark
python -m flatbench compare --engines flatseek --sizes 100000 --schema article

# Compare 3 engines at 1K, 10K, 100K scales
python -m flatbench compare --engines flatseek,elasticsearch --sizes 1000 10000 100000

# Use existing CSV as data source
python -m flatbench compare --engines flatseek,elasticsearch --sizes 100000 --source ./data/my_data.csv
```

---

## Infrastructure

```bash
make up          # Start Elasticsearch
make down        # Stop services (keep data)
make clean       # Destroy volumes
make status       # Check service status
make logs         # View logs
```

| Service | URL |
|---------|-----|
| Elasticsearch | http://localhost:9200 |
| Kibana (dev) | http://localhost:5601 |

---

## Available Engines

| Engine | Index | Search | Aggregate | Range | Wildcard |
|--------|:-----:|:------:|:---------:|:-----:|:--------:|
| **flatseek** | Yes | Yes | Yes | Yes | Yes |
| **elasticsearch** | Yes | Yes | Yes | Yes | Yes |

---

## Available Schemas

| Schema | Fields | Description |
|--------|--------|-------------|
| `article` | 8 | Blog articles: id, title, content, tags, views, published_at, author |
| `standard` | 12 | Generic records: id, name, email, phone, city, country, status, balance, created_at, updated_at, is_verified, tags |
| `ecommerce` | 12 | Order tracking data |
| `logs` | 11 | Log entries: timestamp, level, service, message, etc. |
| `nested` | 6 | Complex nested JSON objects |
| `sosmed` | 9 | Social media posts |
| `devops` | 11 | Infrastructure/monitoring data |
| `adsb` | 10 | Flight tracking data |
| `campaign` | 10 | Marketing campaign data |
| `blockchain` | 9 | Blockchain transaction data |

---

## Benchmark Operations

| Operation | Description | Metrics |
|-----------|-------------|---------|
| **build_index** | Index CSV/JSONL data | duration_ms, rows/sec, index_size_mb, memory_mb |
| **search** | Full-text query (Lucene syntax) | p50_ms, p95_ms, p99_ms, ops/sec |
| **wildcard_search** | Prefix/suffix wildcard queries | p50_ms, p95_ms, ops/sec |
| **range_query** | Numeric/date range filtering | duration_ms, hits, ops/sec |
| **aggregate** | Terms/stats aggregations | duration_ms, bucket_count, ops/sec |

---

## Output

Results are written to `./output/` with timestamps:

```
output/
├── benchmark_20260428_180905.json   # Full structured results
└── benchmark_20260428_180905.md     # Markdown summary
```

---

## Project Structure

```
flatbench/
├── generators/              # Synthetic data generators
│   └── __init__.py
├── runners/                 # Engine runner implementations
│   ├── __init__.py          # BaseRunner + registry + BenchmarkResult
│   ├── flatseek.py          # Flatseek runner
│   └── elasticsearch.py      # Elasticsearch runner
├── benchmarks/              # Benchmark orchestration + report generation
│   └── __init__.py
├── cli.py                   # CLI entry point
├── Makefile                 # Infrastructure management
├── Dockerfile
└── output/                  # Benchmark results
```

---

## Adding a New Engine

```python
from flatbench.runners import BaseRunner, BenchmarkResult, register_engine

@register_engine("myengine")
class MyEngineRunner(BaseRunner):
    name = "myengine"
    supports_aggregate = False
    supports_range_query = True
    supports_wildcard = True

    def build_index(self, data_path: str) -> BenchmarkResult:
        # Indexing logic
        pass

    def search(self, query: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        # Search logic
        pass
```

---

## Benchmark Results Summary

> **Full results:** [`output/benchmark_20260428_180905.md`](output/benchmark_20260428_180905.md)

### Configuration
- **Dataset:** 100,000 rows (article schema)
- **Source file size:** 105.8 MB CSV
- **Engines tested:** flatseek, elasticsearch
- **Iterations per query:** 10

### Build Index

| Engine | Duration | Rows/sec | Index size |
|--------|----------|----------|------------|
| elasticsearch | 17,100 ms | 5,848 | 49.3 MB |
| flatseek | 155,965 ms | 641 | 208.4 MB |

**Winner: Elasticsearch** — 9x faster build, 4x smaller index.

### Search

| Engine | p50 | p95 | Ops/sec |
|--------|-----|-----|---------|
| elasticsearch | 7.60 ms | 66.15 ms | 128 |
| flatseek | **2.31 ms** | 41.81 ms | **247** |

**Winner: flatseek** — 3x faster on median latency, nearly 2x throughput.

### Range Query (correctness verified)

| Query | flatseek hits | ES hits |
|-------|-------------|---------|
| `views [0 TO 100]` | 298 | 298 |
| `views [100 TO 10000]` | 9,711 | 9,711 |
| `views [10000 TO 100000]` | 89,991 | 89,991 |
| `id [1 TO 10000]` | 1,022 | 1,022 |

**Both engines return identical results.** flatseek slightly faster on 3/4 range queries.

### Aggregations (correctness verified)

| Aggregation | flatseek | elasticsearch |
|-------------|----------|---------------|
| `tags` (terms) | 38 buckets | 38 buckets |
| `author` (terms) | 100 buckets | 100 buckets |
| `pub_year` (date_histogram) | 100 buckets | 7 buckets |
| `views_max` (max) | correct | correct |
| `views_min` (min) | correct | correct |
| `views_stats` (stats) | correct | correct |

**flatseek:** 300–700 ms per aggregation. **Elasticsearch:** 5–50 ms — significantly faster due to columnar field data cache.

### Key Takeaways

- **Search:** flatseek wins on text queries (10–28x faster on content/title/tag searches)
- **Build:** Elasticsearch wins (9x faster indexing, 4x smaller index)
- **Range/Aggregate:** Correctness confirmed. ES faster for aggregations; flatseek faster for range queries
- **Wildcard:** flatseek faster for common patterns; ES wins on complex patterns (`*kube*`, `*perform*`)
