"""Pipeline orchestrator: fetch -> process -> load -> pre-compute."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pipeline.config import ensure_data_dirs, get_neo4j_driver, settings
from pipeline.fetchers.fec import (
    download_bulk_file,
    parse_candidate_committee_linkage,
    parse_candidate_master,
    parse_committee_contributions,
    parse_committee_master,
)
from pipeline.fetchers.scorecards import load_all_scorecards
from pipeline.loaders.graph_loader import (
    apply_schema,
    load_candidate_committee_linkage,
    load_candidates,
    load_committee_contributions,
    load_committees,
    load_scorecard_ratings,
    load_seed_data,
)
from pipeline.processors.entity_resolution import (
    filter_corporate_pacs,
    filter_supported_contributions,
)
from pipeline.processors.score_computation import compute_all_scores, export_scores
from pipeline.processors.scorecard_resolver import build_candidate_index, resolve_candidates

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

SCHEMA_PATH = Path("schema/constraints.cypher")
SEED_PATH = Path("schema/seed_issues.cypher")


def run_schema(session) -> None:
    """Step 0: Apply schema constraints and seed data."""
    logger.info("=== Applying schema constraints ===")
    apply_schema(session, SCHEMA_PATH)
    logger.info("=== Loading seed data ===")
    load_seed_data(session, SEED_PATH)


def run_fec(session) -> None:
    """Step 1: Fetch and load FEC Tier 1 bulk data.

    Downloads and processes: committee master (cm), candidate master (cn),
    committee-to-candidate contributions (pas2), and candidate-committee
    linkage (ccl). Individual contributions (indiv) are not processed —
    executive donation tracking is deferred to Tier 2.
    """
    logger.info("=== FEC Data Pipeline ===")

    for cycle in settings.fec_cycles:
        logger.info("--- Processing cycle %d ---", cycle)

        # Download Tier 1 bulk files (no indiv — exec donations deferred to Tier 2)
        cm_path = download_bulk_file("cm", cycle)
        cn_path = download_bulk_file("cn", cycle)
        pas2_path = download_bulk_file("pas2", cycle)
        ccl_path = download_bulk_file("ccl", cycle)

        # Materialize candidates and committees as lists — _batch_load requires list,
        # and we need to iterate each multiple times (load + build sets / filter).
        candidates = list(parse_candidate_master(cn_path))
        committees = list(parse_committee_master(cm_path))

        # Load candidates and committees into Neo4j
        load_candidates(session, candidates)
        load_committees(session, committees)

        # Build set of known candidate IDs for ccl26 validation
        known_cand_ids = {c["candidate_id"] for c in candidates}

        # Filter to corporate PACs and log count
        corporate_pacs = filter_corporate_pacs(committees)
        logger.info(
            "Found %d corporate PACs out of %d committees",
            len(corporate_pacs), len(committees),
        )

        # Stream pas2, filter to support-only transaction types (24K, 24Z),
        # materialize, tag with cycle, then load.
        supported_contribs = list(
            filter_supported_contributions(parse_committee_contributions(pas2_path))
        )
        for c in supported_contribs:
            c["cycle"] = cycle
        load_committee_contributions(session, supported_contribs)
        logger.info("Loaded %d supported contributions for cycle %d", len(supported_contribs), cycle)

        # Load candidate-committee linkage with validation against known candidates
        ccl_rows = parse_candidate_committee_linkage(ccl_path)
        load_candidate_committee_linkage(session, ccl_rows, known_cand_ids=known_cand_ids)


def run_scorecards(session) -> None:
    """Step 2: Load scorecard data, resolve to FEC candidate IDs, and load edges."""
    logger.info("=== Scorecard Pipeline ===")
    raw = load_all_scorecards(settings.scorecard_year)
    index = build_candidate_index(session)
    logger.info("Candidate index built: %d entries", len(index))
    ratings = list(resolve_candidates(raw, index))
    logger.info("Resolved %d scorecard ratings", len(ratings))
    load_scorecard_ratings(session, ratings)


def run_scores(session) -> None:
    """Step 3: Pre-compute and export scores."""
    logger.info("=== Score Computation ===")
    all_scores = compute_all_scores(session)
    output = export_scores(all_scores)
    logger.info("Scores exported to %s", output)


def main():
    parser = argparse.ArgumentParser(description="Political Purchaser data pipeline")
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=["schema", "fec", "scorecards", "scores", "all"],
        default=["all"],
        help="Pipeline steps to run",
    )
    args = parser.parse_args()

    ensure_data_dirs()

    steps = set(args.steps)
    run_all = "all" in steps

    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            if run_all or "schema" in steps:
                run_schema(session)
            if run_all or "fec" in steps:
                run_fec(session)
            if run_all or "scorecards" in steps:
                run_scorecards(session)
            if run_all or "scores" in steps:
                run_scores(session)
    finally:
        driver.close()

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
