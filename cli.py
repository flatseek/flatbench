#!/usr/bin/env python3
"""Flatbench CLI entry point.

Usage:
    # Generate dataset
    python -m flatbench generate --schema standard --rows 100000 --output ./data/test.csv

    # Run benchmark on single engine
    python -m flatbench run --engine flatseek --data ./data/test.csv --index-dir ./index

    # Compare multiple engines
    python -m flatbench compare --engines flatseek,sqlite --sizes 1000 10000 100000

    # List available engines
    python -m flatbench list-engines
"""

import sys
import os

# Add flatbench to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from benchmarks import main

if __name__ == "__main__":
    main()