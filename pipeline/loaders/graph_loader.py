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

    Args:
        candidates: List of dicts with keys from FEC candidate master.
    """
    query = """
    UNWIND $batch AS c
    MERGE (cand:Candidate {fec_candidate_id: c.candidate_id})
    SET cand.name = c.candidate_name,
        cand.party = c.party,
        cand.office = c.office,
        cand.state = c.office_state
    """
    return _batch_load(session, query, candidates)


def load_committees(session: Session, committees: list[dict]) -> int:
    """Load Committee nodes from FEC data.

    Args:
        committees: List of dicts with keys from FEC committee master.
    """
    query = """
    UNWIND $batch AS c
    MERGE (comm:Committee {fec_committee_id: c.committee_id})
    SET comm.name = c.committee_name,
        comm.type = c.type,
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
