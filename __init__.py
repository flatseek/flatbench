#!/usr/bin/env python3
"""Flatbench - Benchmark suite for Flatseek and competitor search engines.

Supports: Flatseek, Elasticsearch, SQLite FTS5, typesense

Usage:
    python -m flatbench.generate --engine flatseek --rows 100000 --output ./data
    python -m flatbench.run --engine flatseek --index ./data/index.csv
    python -m flatbench.compare --engines flatseek,sqlite --size 100k
"""

import sys
import os

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))