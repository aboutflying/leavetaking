"""Load processed data into the Neo4j graph database."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

from neo4j import Session

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def apply_schema(session: Session, schema_path: Path) -> None:
    """Apply schema constraints from a Cypher file."""
    text = schema_path.read_text()
    for statement in text.split(";"):
        statement = statement.strip()
        # Skip empty lines and comments
        if not statement or statement.startswith("//"):
            continue
        # Filter out comment-only lines within a statement
        lines = [line for line in statement.split("\n") if not line.strip().startswith("//")]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            try:
                session.run(cleaned)
                logger.debug("Applied: %s", cleaned[:80])
            except Exception:
                logger.exception("Failed to apply schema statement: %s", cleaned[:80])


def load_seed_data(session: Session, seed_path: Path) -> None:
    """Load seed data (issues, scorecards) from a Cypher file."""
    apply_schema(session, seed_path)
    logger.info("Loaded seed data from %s", seed_path)


def load_brands(session: Session, brands: list[dict]) -> int:
    """Load Brand nodes into the graph.

    Args:
        brands: List of dicts with keys: name, amazon_slug, aliases (optional).

    Returns:
        Number of brands loaded.
    """
    query = """
    UNWIND $batch AS b
    MERGE (brand:Brand {name: b.name})
    SET brand.amazon_slug = b.amazon_slug,
        brand.aliases = b.aliases
    """
    return _batch_load(session, query, brands)


def load_corporations(session: Session, corporations: list[dict]) -> int:
    """Load Corporation nodes.

    Args:
        corporations: List of dicts with keys: name, ticker, cik, jurisdiction, oc_id.
    """
    query = """
    UNWIND $batch AS c
    MERGE (corp:Corporation {name: c.name})
    SET corp.ticker = c.ticker,
        corp.cik = c.cik,
        corp.jurisdiction = c.jurisdiction,
        corp.oc_id = c.oc_id
    """
    return _batch_load(session, query, corporations)


def load_ownership_edges(session: Session, edges: list[dict]) -> int:
    """Load Brand-OWNED_BY->Corporation edges.

    Args:
        edges: List of dicts with keys: brand_name, corporation_name.
    """
    query = """
    UNWIND $batch AS e
    MATCH (b:Brand {name: e.brand_name})
    MATCH (c:Corporation {name: e.corporation_name})
    MERGE (b)-[:OWNED_BY]->(c)
    """
    return _batch_load(session, query, edges)


def load_subsidiary_edges(session: Session, edges: list[dict]) -> int:
    """Load Corporation-SUBSIDIARY_OF->Corporation edges.

    Args:
        edges: List of dicts with keys: child_name, parent_name.
    """
    query = """
    UNWIND $batch AS e
    MATCH (child:Corporation {name: e.child_name})
    MATCH (parent:Corporation {name: e.parent_name})
    MERGE (child)-[:SUBSIDIARY_OF]->(parent)
    """
    return _batch_load(session, query, edges)


def load_candidates(session: Session, candidates: list[dict]) -> int:
    """Load Candidate nodes from FEC data.

    If a candidate's name changes between cycles (e.g. a middle name added or
    dropped), the previous name is appended to ``aliases`` before the golden
    record is updated with the new canonical FEC name.

    Args:
        candidates: List of dicts with keys from FEC candidate master.
    """
    query = """
    UNWIND $batch AS c
    MERGE (cand:Candidate {fec_candidate_id: c.candidate_id})
    WITH cand, c,
         CASE WHEN cand.name IS NOT NULL AND cand.name <> c.candidate_name
              THEN [x IN coalesce(cand.aliases, []) + [cand.name]
                    WHERE NOT x IN coalesce(cand.aliases, [])]
                   + coalesce(cand.aliases, [])
              ELSE coalesce(cand.aliases, [])
         END AS updated_aliases
    SET cand.name = c.candidate_name,
        cand.party = c.party,
        cand.office = c.office,
        cand.state = c.office_state,
        cand.aliases = updated_aliases
    """
    return _batch_load(session, query, candidates)


def load_provisional_candidates(session: Session, provisionals: list[dict]) -> int:
    """Create Candidate nodes for scorecard entries that could not be resolved to FEC IDs.

    Uses MERGE so repeated pipeline runs are idempotent.  Provisional nodes are
    marked with ``provisional: true`` so the FEC reconciliation step can find and
    upgrade them when real FEC data arrives in a future cycle.

    Args:
        provisionals: List of dicts with keys: fec_candidate_id (synthetic PROV_*),
                      candidate_name, state, party (optional).
    """
    query = """
    UNWIND $batch AS p
    MERGE (cand:Candidate {fec_candidate_id: p.fec_candidate_id})
    SET cand.name = p.candidate_name,
        cand.state = p.state,
        cand.party = p.party,
        cand.provisional = true
    """
    return _batch_load(session, query, provisionals)


def reconcile_provisional_candidates(
    session: Session,
    index: dict[tuple[str, str], list[str]],
) -> int:
    """Upgrade provisional Candidate nodes that now have a matching real FEC record.

    Called after ``load_candidates()`` so newly loaded FEC candidates are available.
    For each provisional, looks up the candidate's name in the FEC index.  On a
    match, RATES edges are transferred to the real node, aliases are merged, and
    the provisional node is deleted.

    Args:
        index: (normalized_name, state) -> [fec_candidate_id] from build_candidate_index,
               built *after* the current FEC load so new candidates are included.

    Returns:
        Number of provisional nodes reconciled.
    """
    from pipeline.processors.scorecard_resolver import normalize_scorecard_name

    provisionals = list(
        session.run(
            "MATCH (c:Candidate {provisional: true}) "
            "RETURN c.fec_candidate_id AS prov_id, c.name AS name, c.state AS state"
        )
    )
    if not provisionals:
        return 0

    reconciled = 0
    for record in provisionals:
        prov_id = record["prov_id"]
        name = record["name"] or ""
        state = record["state"] or ""
        normalized = normalize_scorecard_name(name)
        matches = index.get((normalized, state), [])
        if not matches:
            continue

        for real_id in matches:
            session.run(
                """
                MATCH (prov:Candidate {fec_candidate_id: $prov_id})
                MATCH (real:Candidate {fec_candidate_id: $real_id})
                // Transfer RATES edges
                OPTIONAL MATCH (sc:Scorecard)-[r:RATES]->(prov)
                FOREACH (_ IN CASE WHEN r IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (sc)-[nr:RATES]->(real)
                    SET nr.score = r.score, nr.year = r.year
                )
                // Merge aliases: add provisional name and its aliases to real node
                WITH prov, real
                SET real.aliases = [x IN
                    coalesce(real.aliases, [])
                    + coalesce(prov.aliases, [])
                    + [prov.name]
                    WHERE x IS NOT NULL AND x <> real.name
                ]
                WITH prov
                DETACH DELETE prov
                """,
                prov_id=prov_id,
                real_id=real_id,
            )
            logger.info("Reconciled provisional %s → %s (%s)", prov_id, real_id, name)
            reconciled += 1
            break  # one provisional maps to one real candidate

    return reconciled


def load_committees(session: Session, committees: list[dict]) -> int:
    """Load Committee nodes from FEC data.

    Args:
        committees: List of dicts with keys from FEC committee master.

    Properties set:
        type         -- CMTE_TP: committee type (Q=qualified PAC, N=nonqualified PAC,
                        H=House, S=Senate, P=Presidential, O=Super PAC, etc.)
        org_type     -- ORG_TP: connected organization type (C=Corporation,
                        L=Labor, M=Membership, T=Trade association,
                        V=Cooperative, W=Corp without capital stock)
        connected_org -- CONNECTED_ORG_NM: name of the sponsoring organization
    """
    query = """
    UNWIND $batch AS c
    MERGE (comm:Committee {fec_committee_id: c.committee_id})
    SET comm.name = c.committee_name,
        comm.type = c.type,
        comm.org_type = c.interest_group_category,
        comm.connected_org = c.connected_org_name
    """
    return _batch_load(session, query, committees)


def fetch_corporation_names(session: Session) -> list[str]:
    """Return all Corporation.name values currently in the graph."""
    result = session.run("MATCH (c:Corporation) RETURN c.name AS name")
    return [record["name"] for record in result if record["name"]]


def load_pac_edges(session: Session, edges: list[dict]) -> int:
    """Load Corporation-OPERATES_PAC->Committee edges.

    Args:
        edges: List of dicts with keys: corporation_name, committee_id.
    """
    query = """
    UNWIND $batch AS e
    MATCH (corp:Corporation {name: e.corporation_name})
    MATCH (comm:Committee {fec_committee_id: e.committee_id})
    MERGE (corp)-[:OPERATES_PAC]->(comm)
    """
    return _batch_load(session, query, edges)


def load_committee_contributions(session: Session, contributions: list[dict]) -> int:
    """Load Committee-CONTRIBUTED_TO->Candidate edges.

    Uses MERGE on transaction_id so that amended FEC records overwrite originals
    (last-write-wins via SET). This handles the FEC amendment pattern where the
    same TRAN_ID appears multiple times as a filing is corrected.

    Args:
        contributions: List of dicts with keys: committee_id, candidate_id,
                       transaction_id, transaction_amount, transaction_date, cycle.
    """
    query = """
    UNWIND $batch AS c
    MATCH (comm:Committee {fec_committee_id: c.committee_id})
    MATCH (cand:Candidate {fec_candidate_id: c.candidate_id})
    MERGE (comm)-[r:CONTRIBUTED_TO {tran_id: c.transaction_id}]->(cand)
    SET r.amount = toFloat(c.transaction_amount),
        r.date = c.transaction_date,
        r.cycle = c.cycle
    """
    return _batch_load(session, query, contributions)


def load_candidate_committee_linkage(
    session: Session,
    rows: Iterator[dict],
    known_cand_ids: set[str] | None = None,
) -> int:
    """Load Candidate-AUTHORIZED_COMMITTEE->Committee edges from ccl26.

    Validates that each candidate ID in the linkage file has a corresponding
    Candidate node already loaded. Logs a WARNING for any missing candidate IDs.

    Args:
        session: Neo4j session.
        rows: Iterator of dicts from parse_candidate_committee_linkage.
        known_cand_ids: Set of fec_candidate_id values already loaded into Neo4j.
                        If provided, used to validate linkage records.

    Returns:
        Number of linkage records loaded.
    """
    rows_list = list(rows)

    if known_cand_ids is not None:
        for row in rows_list:
            cand_id = row.get("cand_id", "")
            if cand_id not in known_cand_ids:
                logger.warning("CAND_ID %s in ccl26 has no matching Candidate node", cand_id)

    query = """
    UNWIND $batch AS row
    MATCH (cand:Candidate {fec_candidate_id: row.cand_id})
    MATCH (cmte:Committee {fec_committee_id: row.cmte_id})
    MERGE (cand)-[r:AUTHORIZED_COMMITTEE {linkage_id: row.linkage_id}]->(cmte)
    SET r.designation = row.cmte_dsgn,
        r.type = row.cmte_tp
    """
    return _batch_load(session, query, rows_list)


def load_persons(session: Session, persons: list[dict]) -> int:
    """Load Person nodes (executives).

    Args:
        persons: List of dicts with keys: name, title, fec_contributor_id.
    """
    query = """
    UNWIND $batch AS p
    MERGE (person:Person {fec_contributor_id: p.fec_contributor_id})
    SET person.name = p.name,
        person.title = p.title
    """
    return _batch_load(session, query, persons)


def load_executive_edges(session: Session, edges: list[dict]) -> int:
    """Load Person-EXECUTIVE_OF->Corporation edges."""
    query = """
    UNWIND $batch AS e
    MATCH (p:Person {fec_contributor_id: e.fec_contributor_id})
    MATCH (c:Corporation {name: e.corporation_name})
    MERGE (p)-[:EXECUTIVE_OF]->(c)
    """
    return _batch_load(session, query, edges)


def load_individual_donations(session: Session, donations: list[dict]) -> int:
    """Load Person-DONATED_TO->Candidate edges."""
    query = """
    UNWIND $batch AS d
    MATCH (p:Person {fec_contributor_id: d.fec_contributor_id})
    MATCH (cand:Candidate {fec_candidate_id: d.candidate_id})
    CREATE (p)-[:DONATED_TO {
        amount: toFloat(d.transaction_amount),
        date: d.transaction_date,
        cycle: d.cycle
    }]->(cand)
    """
    return _batch_load(session, query, donations)


def load_scorecard_ratings(session: Session, ratings: list[dict]) -> int:
    """Load Scorecard-RATES->Candidate edges.

    Also accumulates scorecard candidate name variants into cand.aliases so the
    golden record (FEC name) retains all alternate names seen across sources.

    Args:
        ratings: List of dicts with keys: org_name, year, fec_candidate_id, score,
                 and optionally candidate_name (the name as it appeared in the scorecard).
    """
    query = """
    UNWIND $batch AS r
    MATCH (sc:Scorecard {org_name: r.org_name})
    MATCH (cand:Candidate {fec_candidate_id: r.fec_candidate_id})
    MERGE (sc)-[rate:RATES]->(cand)
    SET rate.score = toFloat(r.score),
        rate.year = r.year
    WITH cand, r
    WHERE r.candidate_name IS NOT NULL
      AND NOT r.candidate_name IN coalesce(cand.aliases, [])
    SET cand.aliases = coalesce(cand.aliases, []) + r.candidate_name
    """
    return _batch_load(session, query, ratings)


def is_fec_cycle_loaded(session: Session, cycle: int) -> bool:
    """Return True if a FECCycleLoad marker node exists for this cycle.

    The marker is written by mark_fec_cycle_loaded() at the end of a successful
    run_fec() cycle.  Its presence means all four Tier 1 files (cm, cn, pas2, ccl)
    were fully processed and loaded for that cycle.
    """
    result = session.run(
        "MATCH (n:FECCycleLoad {cycle: $cycle}) RETURN count(n) > 0 AS loaded",
        cycle=cycle,
    )
    return bool(result.single()["loaded"])


def mark_fec_cycle_loaded(session: Session, cycle: int) -> None:
    """Write (or update) a FECCycleLoad marker node for this cycle."""
    session.run(
        "MERGE (n:FECCycleLoad {cycle: $cycle}) SET n.loaded_at = datetime()",
        cycle=cycle,
    )


_LOAD_LOG_INTERVAL = 50_000


def _batch_load(session: Session, query: str, records: list[dict]) -> int:
    """Execute a batched UNWIND load query."""
    n = len(records)
    total = 0
    for i in range(0, n, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        session.run(query, batch=batch)
        total += len(batch)
        if n >= _LOAD_LOG_INTERVAL and total % _LOAD_LOG_INTERVAL == 0:
            logger.info("  loaded %d / %d records (%.0f%%)", total, n, total / n * 100)
    logger.info("Loaded %d records", total)
    return total
