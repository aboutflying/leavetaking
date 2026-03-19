"""Standalone entry point for brand resolution.

Delegates entirely to run_pipeline.run_brands(). Use this script to run brand
resolution independently of the full pipeline, e.g. when Neo4j is running but
you don't want to re-run FEC/scorecard steps.

Usage:
    python scripts/bootstrap_brands.py
    python scripts/bootstrap_brands.py --interactive   # prompt for low-confidence brands
"""

import argparse

from pipeline.config import ensure_data_dirs, get_neo4j_driver
from pipeline.run_pipeline import run_brands


def main():
    parser = argparse.ArgumentParser(description="Resolve brand names to corporate entities")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Pause and prompt for manual brand matching when no confident match is found",
    )
    args = parser.parse_args()

    ensure_data_dirs()
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            run_brands(session, interactive=args.interactive)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
