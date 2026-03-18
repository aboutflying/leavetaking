"""Pipeline orchestrator: fetch -> process -> load -> pre-compute."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from pipeline.config import ensure_data_dirs, get_neo4j_driver, settings
from pipeline.fetchers.fec import (
    download_bulk_file,
    parse_candidate_master,
    parse_committee_contributions,
    parse_committee_master,
    parse_individual_contributions,
)
from pipeline.fetchers.scorecards import load_all_manual_scorecards
from pipeline.loaders.graph_loader import (
    apply_schema,
    load_candidates,
    load_committee_contributions,
    load_committees,
    load_individual_donations,
    load_pac_edges,
    load_persons,
    load_scorecard_ratings,
    load_seed_data,
)
from pipeline.processors.entity_resolution import (
    filter_corporate_pacs,
    filter_executive_donations,
)
from pipeline.processors.score_computation import compute_all_scores, export_scores

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
    """Step 1: Fetch and load FEC data."""
    logger.info("=== FEC Data Pipeline ===")

    for cycle in settings.fec_cycles:
        logger.info("--- Processing cycle %d ---", cycle)

        # Download and parse bulk files
        cm_path = download_bulk_file("cm", cycle)
        cn_path = download_bulk_file("cn", cycle)
        pas2_path = download_bulk_file("pas2", cycle)
        indiv_path = download_bulk_file("indiv", cycle)

        committees = parse_committee_master(cm_path)
        candidates = parse_candidate_master(cn_path)
        committee_contribs = parse_committee_contributions(pas2_path)
        individual_contribs = parse_individual_contributions(indiv_path)

        # Load candidates and committees
        load_candidates(session, candidates)
        load_committees(session, committees)

        # Filter to corporate PACs and load contributions
        corporate_pacs = filter_corporate_pacs(committees)
        logger.info("Found %d corporate PACs out of %d committees", len(corporate_pacs), len(committees))

        # Add cycle info to contributions
        for c in committee_contribs:
            c["cycle"] = cycle
        load_committee_contributions(session, committee_contribs)

        # Filter executive donations and load
        exec_donations = filter_executive_donations(individual_contribs)
        logger.info(
            "Found %d executive donations out of %d individual contributions",
            len(exec_donations), len(individual_contribs),
        )
        for d in exec_donations:
            d["cycle"] = cycle
            d["fec_contributor_id"] = f"{d.get('contributor_name', '')}_{d.get('zip', '')}"
            d["candidate_id"] = d.get("other_id", "")

        # Create Person nodes for executives
        persons = []
        seen = set()
        for d in exec_donations:
            fid = d["fec_contributor_id"]
            if fid not in seen:
                persons.append({
                    "name": d.get("contributor_name", ""),
                    "title": d.get("occupation", ""),
                    "fec_contributor_id": fid,
                })
                seen.add(fid)
        load_persons(session, persons)
        load_individual_donations(session, exec_donations)


def run_scorecards(session) -> None:
    """Step 2: Load scorecard data."""
    logger.info("=== Scorecard Pipeline ===")
    records = load_all_manual_scorecards()
    logger.info("Loaded %d scorecard ratings", len(records))

    ratings = [
        r.to_dict() for r in records
        if r.fec_candidate_id  # Only load ratings we can link to candidates
    ]
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
