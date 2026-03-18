"""Export pre-computed scores from Neo4j to JSON for the browser extension."""

from __future__ import annotations

import argparse
import logging

from pipeline.config import get_neo4j_driver, settings
from pipeline.processors.score_computation import compute_all_scores, export_scores

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Export pre-computed scores")
    parser.add_argument(
        "--output",
        type=str,
        default=str(settings.scores_output),
        help="Output path for scores JSON",
    )
    args = parser.parse_args()

    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            scores = compute_all_scores(session)
            output = export_scores(scores, output_path=args.output)
            logger.info("Exported %d brand scores to %s", len(scores), output)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
