"""Flatbench CLI - Command line interface."""

import sys


def main():
    from flatbench.benchmarks import main as benchmarks_main
    sys.argv[0] = "flatbench"
    benchmarks_main()


if __name__ == "__main__":
    main()
