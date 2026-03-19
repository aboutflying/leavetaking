"""Pipeline orchestrator: fetch -> process -> load -> pre-compute."""

from __future__ import annotations

import argparse
import logging
import time
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
from pipeline.fetchers.wikidata import (
    _search_entities,
    discover_brands_for_corporation,
    get_ownership_chain,
    get_subsidiaries,
)
from pipeline.loaders.graph_loader import (
    apply_schema,
    fetch_corporate_pacs_from_graph,
    fetch_corporation_names,
    is_fec_cycle_loaded,
    load_brands,
    load_candidate_committee_linkage,
    load_candidates,
    load_committee_contributions,
    load_committees,
    load_corporations,
    load_ownership_edges,
    load_pac_edges,
    load_provisional_candidates,
    load_scorecard_ratings,
    load_seed_data,
    load_subsidiary_edges,
    mark_fec_cycle_loaded,
    reconcile_provisional_candidates,
)
from pipeline.processors.brand_resolver import resolve_all_brands
from pipeline.processors.entity_resolution import (
    filter_corporate_pacs,
    filter_supported_contributions,
    resolve_pac_to_corporation,
    similarity,
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
FORTUNE100_SEED_PATH = Path("schema/seed_fortune100.cypher")

# Top Amazon brands for MVP. In production these would be scraped/crawled.
TOP_BRANDS = [
    "Amazon Basics",
    "Apple",
    "Samsung",
    "Sony",
    "LG",
    "Bose",
    "Nike",
    "Adidas",
    "Under Armour",
    "Levi's",
    "Hasbro",
    "Mattel",
    "Procter & Gamble",
    "Unilever",
    "Johnson & Johnson",
    "Colgate-Palmolive",
    "Nestle",
    "PepsiCo",
    "Coca-Cola",
    "General Mills",
    "Kellogg's",
    "Kraft Heinz",
    "Mars",
    "Mondelez",
    "Hershey",
    "Tyson Foods",
    "3M",
    "Honeywell",
    "General Electric",
    "Whirlpool",
    "Black & Decker",
    "Duracell",
    "Energizer",
    "Clorox",
    "SC Johnson",
    "Church & Dwight",
    "Estee Lauder",
    "L'Oreal",
    "Revlon",
    "Maybelline",
    "Microsoft",
    "Google",
    "Intel",
    "AMD",
    "NVIDIA",
    "HP",
    "Dell",
    "Lenovo",
    "ASUS",
    "Acer",
]


def run_schema(session) -> None:
    """Step 0: Apply schema constraints and seed data."""
    logger.info("=== Applying schema constraints ===")
    apply_schema(session, SCHEMA_PATH)
    logger.info("=== Loading seed data ===")
    load_seed_data(session, SEED_PATH)
    logger.info("=== Loading Fortune 100 corporations ===")
    load_seed_data(session, FORTUNE100_SEED_PATH)


def run_brands(
    session,
    interactive: bool = False,
    retry_nulls: bool = False,
    skip_discovery: bool = False,
) -> None:
    """Step 1: Resolve brand names to corporations and load into the graph.

    Must run before run_fec so that Corporation nodes exist for PAC linkage.
    Results are cached to settings.data_dir/brand_resolutions.json — re-runs
    skip already-resolved brands.

    Args:
        interactive: When True, pause and prompt the user for brands that have
                     candidates but no confident automatic match. When False
                     (default), below-threshold brands are silently skipped —
                     safe for unattended/cron runs.
        skip_discovery: When True, skip QID enrichment and subsidiary/brand
                        discovery phases (faster for dev re-runs).
    """
    from pipeline.processors.brand_resolver import _stdin_prompt

    logger.info("=== Brand Resolution Pipeline ===")
    cache_path = settings.data_dir / "brand_resolutions.json"
    prompt_fn = _stdin_prompt if interactive else None
    resolutions = resolve_all_brands(TOP_BRANDS, cache_path, prompt_fn=prompt_fn, retry_nulls=retry_nulls)

    brands = [
        {"name": name, "amazon_slug": name.lower().replace(" ", "-"), "aliases": []}
        for name in TOP_BRANDS
    ]
    load_brands(session, brands)

    corporations: list[dict] = []
    ownership_edges: list[dict] = []
    subsidiary_edges: list[dict] = []

    for brand_name, match in resolutions.items():
        corp_name = match["name"]
        corporations.append(
            {
                "name": corp_name,
                "ticker": match.get("ticker"),
                "cik": None,
                "jurisdiction": match.get("jurisdiction"),
                "oc_id": str(match["oc_id"]) if match.get("oc_id") else None,
            }
        )
        ownership_edges.append({"brand_name": brand_name, "corporation_name": corp_name})

        if match.get("qid"):
            try:
                chain = get_ownership_chain(match["qid"])
                for link in chain:
                    if not link.get("parent_name") or not link.get("child_name"):
                        continue  # Wikidata sometimes returns empty labels — skip
                    corporations.append(
                        {
                            "name": link["parent_name"],
                            "ticker": None,
                            "cik": None,
                            "jurisdiction": None,
                            "oc_id": None,
                        }
                    )
                    subsidiary_edges.append(
                        {
                            "child_name": link["child_name"],
                            "parent_name": link["parent_name"],
                        }
                    )
            except Exception:
                logger.exception("Ownership chain failed for %s (%s)", brand_name, match["qid"])

    # Deduplicate corporations by name before loading
    seen: set[str] = set()
    unique_corps: list[dict] = []
    for c in corporations:
        if c["name"] not in seen:
            unique_corps.append(c)
            seen.add(c["name"])

    load_corporations(session, unique_corps)
    load_ownership_edges(session, ownership_edges)
    load_subsidiary_edges(session, subsidiary_edges)
    logger.info(
        "Loaded %d brands, %d corporations, %d ownership edges, %d subsidiary edges",
        len(brands),
        len(unique_corps),
        len(ownership_edges),
        len(subsidiary_edges),
    )

    if not skip_discovery:
        logger.info("=== QID Enrichment ===")
        enrich_corporation_qids(session)
        logger.info("=== Subsidiary Discovery ===")
        discover_subsidiaries_for_corpus(session)
        logger.info("=== Brand Discovery ===")
        discover_brands_for_corpus(session)
        logger.info("=== Corporation Deduplication ===")
        deduplicate_corporations_by_qid(session)


def enrich_corporation_qids(session, delay: float = 1.0) -> int:
    """Backfill Wikidata QIDs for Corporation nodes that lack one.

    Queries wbsearchentities for each corporation name, accepts the first hit
    with similarity(corp_name, label) >= 0.7. Idempotent: the Neo4j query
    filters to WHERE c.qid IS NULL so already-enriched corps are skipped.
    Rate-limited via delay between API calls.

    Returns:
        Number of corporations successfully enriched with a QID.
    """
    result = session.run(
        "MATCH (c:Corporation) WHERE c.qid IS NULL RETURN c.name AS name"
    )
    names = [r["name"] for r in result]
    enriched = 0
    for name in names:
        try:
            hits = _search_entities(name)
        except Exception:
            logger.warning("QID enrichment search failed for '%s'", name)
            time.sleep(delay)
            continue
        for hit in hits:
            label = hit.get("label", "")
            if label and similarity(name, label) >= 0.7:
                load_corporations(session, [{"name": name, "qid": hit["qid"]}])
                enriched += 1
                break
        time.sleep(delay)
    logger.info("Enriched %d corporation QIDs", enriched)
    return enriched


def discover_subsidiaries_for_corpus(session, delay: float = 1.0) -> int:
    """Discover subsidiaries for Corporation nodes that have a Wikidata QID.

    Queries Neo4j for all Corporation nodes with a qid, calls get_subsidiaries()
    for each, and loads the results as new Corporation nodes + SUBSIDIARY_OF edges.
    Idempotent: Neo4j MERGE prevents duplicate nodes/edges on re-runs.
    Rate-limited via delay between API calls.

    Returns:
        Total number of subsidiary Corporation nodes loaded.
    """
    result = session.run(
        "MATCH (c:Corporation) WHERE c.qid IS NOT NULL RETURN c.name AS name, c.qid AS qid"
    )
    corps = [(r["name"], r["qid"]) for r in result if r["qid"]]
    total = 0
    for corp_name, qid in corps:
        try:
            subs = get_subsidiaries(qid)
        except Exception:
            logger.exception("get_subsidiaries failed for %s (%s)", corp_name, qid)
            time.sleep(delay)
            continue
        new_corps = []
        new_edges = []
        for s in subs:
            if not s.get("name"):
                continue
            new_corps.append(
                {
                    "name": s["name"],
                    "ticker": None,
                    "cik": None,
                    "jurisdiction": None,
                    "oc_id": None,
                }
            )
            new_edges.append({"child_name": s["name"], "parent_name": corp_name})
        if new_corps:
            load_corporations(session, new_corps)
            load_subsidiary_edges(session, new_edges)
            total += len(new_corps)
        time.sleep(delay)
    logger.info("Discovered %d subsidiaries across %d corporations", total, len(corps))
    return total


def discover_brands_for_corpus(session, delay: float = 1.0) -> int:
    """Discover consumer brands owned by Corporation nodes that have a Wikidata QID.

    Uses reverse P749 (parent organization) filtered to non-Q4830453 entities.
    The FILTER NOT EXISTS { P31/P279* Q4830453 } pattern can be slow on Wikidata
    for large corporations — expect this phase to run 2-5 minutes for a full corpus.

    Returns:
        Total number of Brand nodes loaded.
    """
    result = session.run(
        "MATCH (c:Corporation) WHERE c.qid IS NOT NULL RETURN c.name AS name, c.qid AS qid"
    )
    corps = [(r["name"], r["qid"]) for r in result if r["qid"]]
    total = 0
    for corp_name, qid in corps:
        try:
            brands = discover_brands_for_corporation(qid)
        except Exception:
            logger.exception(
                "discover_brands_for_corporation failed for %s (%s)", corp_name, qid
            )
            time.sleep(delay)
            continue
        new_brands = []
        new_edges = []
        for b in brands:
            if not b.get("name"):
                continue
            new_brands.append(
                {
                    "name": b["name"],
                    "amazon_slug": b["name"].lower().replace(" ", "-"),
                    "aliases": None,  # preserve any existing aliases via CASE guard in load_brands
                }
            )
            new_edges.append({"brand_name": b["name"], "corporation_name": corp_name})
        if new_brands:
            load_brands(session, new_brands)
            load_ownership_edges(session, new_edges)
            total += len(new_brands)
        time.sleep(delay)
    logger.info("Discovered %d brands across %d corporations", total, len(corps))
    return total


def deduplicate_corporations_by_qid(session) -> int:
    """Merge Corporation nodes that share the same Wikidata QID.

    Picks the node with the most relationships as canonical (ties broken by
    name ascending). Stores all duplicate names — and their existing aliases —
    in canonical.aliases. Re-homes OWNED_BY, SUBSIDIARY_OF (both directions),
    and OPERATES_PAC edges from each duplicate to canonical, then DETACH DELETEs
    the duplicate.

    Idempotent: returns 0 when no duplicates exist.

    Returns:
        Number of duplicate Corporation nodes removed.
    """
    groups_result = session.run("""
        MATCH (c:Corporation)
        WHERE c.qid IS NOT NULL
        WITH c.qid AS qid, collect(c.name) AS names
        WHERE size(names) > 1
        RETURN qid, names
    """)
    groups = [(r["qid"], r["names"]) for r in groups_result]

    total_removed = 0
    for qid, _ in groups:
        ranked = list(session.run("""
            MATCH (c:Corporation {qid: $qid})
            RETURN c.name AS name,
                   coalesce(c.aliases, []) AS aliases,
                   size([(c)-[]-() | 1]) AS rel_count
            ORDER BY rel_count DESC, c.name ASC
        """, qid=qid))

        if len(ranked) < 2:
            continue

        canonical_name = ranked[0]["name"]
        for dup in ranked[1:]:
            dup_name = dup["name"]
            dup_aliases = dup["aliases"]

            # Re-home OWNED_BY (Brand → dup → canonical)
            session.run("""
                MATCH (b:Brand)-[:OWNED_BY]->(dup:Corporation {name: $dup_name})
                MATCH (canonical:Corporation {name: $canonical_name})
                MERGE (b)-[:OWNED_BY]->(canonical)
            """, dup_name=dup_name, canonical_name=canonical_name)

            # Re-home SUBSIDIARY_OF where dup is child
            session.run("""
                MATCH (dup:Corporation {name: $dup_name})-[:SUBSIDIARY_OF]->(parent:Corporation)
                MATCH (canonical:Corporation {name: $canonical_name})
                WHERE canonical <> parent
                MERGE (canonical)-[:SUBSIDIARY_OF]->(parent)
            """, dup_name=dup_name, canonical_name=canonical_name)

            # Re-home SUBSIDIARY_OF where dup is parent
            session.run("""
                MATCH (dup:Corporation {name: $dup_name})<-[:SUBSIDIARY_OF]-(child:Corporation)
                MATCH (canonical:Corporation {name: $canonical_name})
                WHERE canonical <> child
                MERGE (child)-[:SUBSIDIARY_OF]->(canonical)
            """, dup_name=dup_name, canonical_name=canonical_name)

            # Re-home OPERATES_PAC
            session.run("""
                MATCH (dup:Corporation {name: $dup_name})-[:OPERATES_PAC]->(cmte:Committee)
                MATCH (canonical:Corporation {name: $canonical_name})
                MERGE (canonical)-[:OPERATES_PAC]->(cmte)
            """, dup_name=dup_name, canonical_name=canonical_name)

            # Accumulate dup name + dup aliases onto canonical (skip canonical.name itself)
            new_aliases = [dup_name] + dup_aliases
            session.run("""
                MATCH (canonical:Corporation {name: $canonical_name})
                SET canonical.aliases = reduce(
                    acc = coalesce(canonical.aliases, []),
                    x IN $new_aliases |
                    CASE WHEN x IN acc OR x = canonical.name THEN acc ELSE acc + [x] END
                )
            """, canonical_name=canonical_name, new_aliases=new_aliases)

            # Delete duplicate
            session.run("""
                MATCH (dup:Corporation {name: $dup_name})
                DETACH DELETE dup
            """, dup_name=dup_name)

            logger.info(
                "Merged duplicate Corporation '%s' -> '%s' (QID: %s)",
                dup_name, canonical_name, qid,
            )
            total_removed += 1

    logger.info("Deduplicated %d Corporation nodes by QID", total_removed)
    return total_removed


def run_fec(session, force: bool = False) -> None:
    """Step 1: Fetch and load FEC Tier 1 bulk data.

    Downloads and processes: committee master (cm), candidate master (cn),
    committee-to-candidate contributions (pas2), and candidate-committee
    linkage (ccl). Individual contributions (indiv) are not processed —
    executive donation tracking is deferred to Tier 2.

    After loading candidates, runs reconciliation to upgrade any provisional
    Candidate nodes (created by the scorecard pipeline) that now have a
    matching real FEC record.

    Cycles that have already been loaded (FECCycleLoad marker present) are
    skipped unless ``force=True``.
    """
    logger.info("=== FEC Data Pipeline ===")

    for cycle in settings.fec_cycles:
        if not force and is_fec_cycle_loaded(session, cycle):
            logger.info("Cycle %d already loaded — skipping (use --force to reload)", cycle)
            continue
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

        # Upgrade any provisional Candidate nodes that now match real FEC records.
        index = build_candidate_index(session)
        n_reconciled = reconcile_provisional_candidates(session, index)
        if n_reconciled:
            logger.info("Reconciled %d provisional candidates to FEC records", n_reconciled)

        # Build set of known candidate IDs for ccl26 validation
        known_cand_ids = {c["candidate_id"] for c in candidates}

        # Filter to corporate PACs and log count
        corporate_pacs = filter_corporate_pacs(committees)
        logger.info(
            "Found %d corporate PACs out of %d committees",
            len(corporate_pacs),
            len(committees),
        )

        # Stream pas2, filter to support-only transaction types (24K, 24Z),
        # materialize, tag with cycle, then load.
        supported_contribs = list(
            filter_supported_contributions(parse_committee_contributions(pas2_path))
        )
        for c in supported_contribs:
            c["cycle"] = cycle
        load_committee_contributions(session, supported_contribs)
        logger.info(
            "Loaded %d supported contributions for cycle %d", len(supported_contribs), cycle
        )

        # Load candidate-committee linkage with validation against known candidates
        ccl_rows = parse_candidate_committee_linkage(ccl_path)
        load_candidate_committee_linkage(session, ccl_rows, known_cand_ids=known_cand_ids)

        mark_fec_cycle_loaded(session, cycle)
        logger.info("Cycle %d load complete", cycle)


def run_pac_linkage(session) -> None:
    """Step 2b: Link corporate PACs to Corporation nodes.

    Reads Committee nodes already in the graph (loaded by run_fec), matches
    their connected_org name against Corporation names and aliases, and writes
    OPERATES_PAC edges. Runs in seconds — no network calls, no file downloads.

    Decoupled from run_fec so it can be re-run independently after brand
    resolution adds or renames Corporation nodes without re-downloading FEC data.

    Requires run_fec to have run first (Committee nodes must exist).
    Requires run_brands to have run first (Corporation nodes must exist).
    """
    logger.info("=== PAC Linkage Pipeline ===")
    corporate_pacs = fetch_corporate_pacs_from_graph(session)
    logger.info("Found %d corporate PAC committees in graph", len(corporate_pacs))

    corp_names = fetch_corporation_names(session)
    if not corp_names:
        logger.warning(
            "No Corporation nodes found — skipping PAC linkage. Run brand resolution first."
        )
        return

    pac_edges = resolve_pac_to_corporation(corporate_pacs, corp_names)
    load_pac_edges(session, pac_edges)
    logger.info(
        "Linked %d corporate PACs to corporations (%d unmatched)",
        len(pac_edges),
        len(corporate_pacs) - len(pac_edges),
    )


def run_scorecards(session) -> None:
    """Step 2: Load scorecard data, resolve to FEC candidate IDs, and load edges.

    Candidates that cannot be resolved to an FEC ID are created as provisional
    Candidate nodes so their scorecard ratings are preserved.  They will be
    upgraded to real FEC records the next time run_fec() runs.
    """
    logger.info("=== Scorecard Pipeline ===")
    raw = load_all_scorecards(settings.scorecard_year)
    index = build_candidate_index(session)
    logger.info("Candidate index built: %d entries", len(index))
    ratings = list(resolve_candidates(raw, index))
    logger.info("Resolved %d scorecard ratings", len(ratings))

    provisionals = [r for r in ratings if r.get("provisional")]
    if provisionals:
        logger.info("Creating %d provisional candidate nodes", len(provisionals))
        load_provisional_candidates(session, provisionals)

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
        choices=["schema", "brands", "fec", "pac_linkage", "scorecards", "scores", "all"],
        default=["all"],
        help="Pipeline steps to run",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-load FEC cycles even if they are already marked as loaded in the graph",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Pause and prompt for manual brand matching when no confident match is found",
    )
    parser.add_argument(
        "--retry-nulls",
        action="store_true",
        help="Re-resolve brands previously cached as null (no match found)",
    )
    parser.add_argument(
        "--skip-discovery",
        action="store_true",
        help="Skip QID enrichment and subsidiary/brand discovery phases (faster for dev re-runs)",
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
            if run_all or "brands" in steps:
                run_brands(
                    session,
                    interactive=args.interactive,
                    retry_nulls=args.retry_nulls,
                    skip_discovery=args.skip_discovery,
                )
            if run_all or "fec" in steps:
                run_fec(session, force=args.force)
            if run_all or "pac_linkage" in steps:
                run_pac_linkage(session)
            if run_all or "scorecards" in steps:
                run_scorecards(session)
            if run_all or "scores" in steps:
                run_scores(session)
    finally:
        driver.close()

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()
