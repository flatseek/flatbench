<div align="center">

<img src="logo.svg" alt="Flatbench" width="64" height="64">

# Flatbench

**Search engine benchmark suite — compare Flatseek against Elasticsearch, tantivy, Typesense, Whoosh, ZincSearch, SQLite, and DuckDB.**

Benchmarks: build speed, search latency, wildcard, range queries, and aggregations. Results saved as JSON + Markdown to `./output/`.

</div>

---

## Install

```bash
pip install flatbench
```

Requires Python ≥ 3.10, Docker (for full engine comparison).

---

## Quick Start

### 1. Start all search engines (Docker)

```bash
make up
```

Starts: Flatseek API (port 8000), Elasticsearch (9200), Typesense (8108), ZincSearch (4080).

### 2. Generate a dataset

```bash
flatbench generate --schema article --rows 500000 -o ./data/article.csv
```

### 3. Run benchmark comparison

```bash
flatbench compare --engines flatseek_cli,elasticsearch,tantivy,typesense,whoosh,zincsearch --sizes 500000 --schema article
```

Results → `output/benchmark_YYYYMMDD_HHMMSS.json` + `.md`.

---

## CLI Reference

### Commands

| Command | Description |
|---------|-------------|
| `flatbench generate` | Generate synthetic dataset |
| `flatbench compare` | Compare multiple engines |
| `flatbench run` | Benchmark single engine |
| `flatbench serve` | Serve report viewer locally |

### Generate

```bash
flatbench generate --schema <schema> --rows <N> --output <path> [--format csv|jsonl]
```

### Compare

```bash
flatbench compare --engines <engines> --sizes <sizes> [options]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `--schema` | Data schema | `standard` |
| `--workers` | Parallel index workers | `1` |
| `--format` | `csv` or `jsonl` | `csv` |
| `--source` | Use existing CSV/JSONL instead of generating | — |
| `--mode` | `normal` (disk) or `tmpfs` (RAM) | `normal` |
| `--cache-dir` | Cache generated data for reuse | — |
| `--skip-build` | Skip build (use existing index) | — |

**Engines:** `flatseek`, `flatseek_cli`, `elasticsearch`, `tantivy`, `typesense`, `whoosh`, `zincsearch`, `sqlite`, `duckdb`

**Sizes:** multiple sizes supported, e.g. `--sizes 1000 10000 500000`

### Examples

```bash
# Generate article dataset (500K rows)
flatbench generate --schema article --rows 500000 -o ./data/article.csv

# Compare at single scale
flatbench compare --engines flatseek_cli,elasticsearch --sizes 500000

# Compare at multiple scales
flatbench compare --engines flatseek,tantivy --sizes 1000 10000 500000

# Use existing CSV (reuse generated data)
flatbench compare --engines flatseek,elasticsearch --sizes 500000 --source ./data/article.csv

# RAM-backed index (tmpfs mode, faster builds)
flatbench compare --engines flatseek,tantivy --sizes 500000 --mode tmpfs
```

---

## Infrastructure (Makefile)

```bash
make up           # Start all services (docker-compose up -d)
make down         # Stop services (keep volumes)
make clean        # Stop and remove volumes
make status       # Show service status
make logs         # View logs (follow mode)

# Flatseek management
make fs-health    # Health check
make fs-stats     # Index stats
make fs-create    # Create index
make fs-delete    # Delete index

# Elasticsearch management
make es-health    # Cluster health
make es-stats     # Cluster stats
make es-create    # Create index
make es-delete    # Delete index

# Typesense management
make ts-health    # Health check
make ts-stats     # Collection stats
make ts-create    # Create collection
make ts-delete    # Delete collection

# ZincSearch management
make zs-health    # Health check
make zs-stats     # Index stats
make zs-create    # Create index
make zs-delete    # Delete index

# Run benchmark directly via Make
make benchmark NROWS=500000 ENGINES="flatseek_cli,elasticsearch,tantivy"
```

**Service URLs:**

| Service | URL |
|---------|-----|
| Flatseek API | http://localhost:8000 |
| Elasticsearch | http://localhost:9200 |
| Typesense | http://localhost:8108 |
| ZincSearch | http://localhost:4080 |
| Kibana | http://localhost:5601 (dev profile) |

---

## Available Schemas

| Schema | Fields | Description |
|--------|--------|-------------|
| `article` | 8 | Blog articles: id, title, content, tags, views, published_at, author |
| `standard` | 12 | Generic: id, name, email, phone, city, country, status, balance, created_at, updated_at, is_verified, tags |
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
| `build_index` | Bulk API indexing (1000 rows/batch) | duration_ms, rows/sec, index_size_mb |
| `search` | Full-text query | p50_ms, p95_ms, p99_ms, ops/sec |
| `wildcard_search` | Prefix/suffix wildcard queries | p50_ms, p95_ms, ops/sec |
| `range_query` | Numeric/date range filtering | duration_ms, hits, ops/sec |
| `aggregate` | Terms/stats aggregations | duration_ms, bucket_count, ops/sec |

---

## Output

Results written to `./output/` with timestamps:

```
output/
├── benchmark_20260501_142947.json   # Full structured results
├── benchmark_20260501_142947.md     # Markdown summary
└── index.json                        # Report manifest (for web viewer)
```

### Report Viewer

**Live:** [bench.flatseek.io](https://bench.flatseek.io) — hosted Flatbench report viewer.

**Local:** Run `flatbench serve --port 8080` or open `report_viewer.html` directly in browser.

<p align="center">
  <img src="flatbench-report-preview.png" alt="Flatbench Report Viewer" width="100%" />
</p>

---

## Build Static Site

Build output directory as a static site (for self-hosted or Vercel deploy):

```bash
make build
# or
bash build.sh
```

Output → `public/` directory with `index.html`, `output/*.json`, `output/*.md`.

### Deploy to Vercel

```bash
make deploy        # Deploy to production (flatbench.vercel.app)
make deploy-preview  # Deploy preview build
```

---

## Project Structure

```
flatbench/
├── Dockerfile              # Flatseek API server container
├── docker-compose.yml       # All engine containers
├── Makefile                 # Infrastructure + build commands
├── build.sh                 # Static site build script
├── report_viewer.html       # Web UI for browsing results
├── pyproject.toml           # flatbench package definition
├── src/flatbench/
│   ├── cli.py               # CLI entry point
│   ├── benchmarks/           # Benchmark orchestration + report generation
│   ├── generators/           # Synthetic data generators (schema-aware)
│   ├── runners/              # Engine runners (HTTP API / CLI)
│   │   ├── flatseek_api.py   # Flatseek HTTP API runner
│   │   ├── flatseek_cli.py   # Flatseek CLI runner
│   │   ├── elasticsearch.py   # Elasticsearch runner
│   │   ├── tantivy.py        # tantivy (Rust) runner
│   │   ├── typesense.py      # Typesense runner
│   │   ├── whoosh.py         # Whoosh runner
│   │   ├── zincsearch.py     # ZincSearch runner
│   │   ├── sqlite.py         # SQLite FTS5 runner
│   │   └── duckdb.py         # DuckDB full-text runner
│   └── output/               # Benchmark results (JSON + Markdown)
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

    def build_index(self, data_path: str, **kwargs) -> BenchmarkResult:
        # Bulk API indexing logic
        pass

    def search(self, query: str, iterations: int = 10, **kwargs) -> BenchmarkResult:
        # Search via HTTP API
        pass
```

Then add to `--engines` list: `--engines flatseek,myengine,...`

---

## Benchmark Results (Latest: 500K rows, article schema)

> **Full results:** [`output/benchmark_20260501_142947.md`](output/benchmark_20260501_142947.md)

### Overall Score (60% speed · 40% correctness)

| Engine | Speed | Correctness | Score |
|--------|-------|-------------|-------|
| **Flatseek** | 🟢 | 🟢 | **0.878** ◀ |
| typesense | 🟢 | 🟢 | 0.832 |
| zincsearch | 🟢 | 🟢 | 0.823 |
| elasticsearch | 🟢 | 🟢 | 0.820 |
| tantivy | 🟢 | 🔴 | 0.650 |
| whoosh | 🔴 | 🔴 | 0.025 |

### Key Takeaways

- **Correctness matters:** Flatseek is the only engine with zero correctness errors. Tantivy misses 99.4% of range query hits.
- **Search:** Tantivy fastest (0.7ms p50), but wrong. Flatseek second-fastest correct (7.9ms).
- **Build:** Tantivy wins (21s for 500K), but Flatseek build is reasonable (217s).
- **Aggregation:** Competitors (ES, tantivy) are 20–300× faster — Flatseek aggregation is a known weakness.