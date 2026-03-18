"""Load processed data into the Neo4j graph database."""

from __future__ import annotations

import logging
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
        lines = [l for l in statement.split("\n") if not l.strip().startswith("//")]
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

    Args:
        contributions: List of dicts with keys: committee_id, candidate_id,
                       transaction_amount, transaction_date.
    """
    query = """
    UNWIND $batch AS c
    MATCH (comm:Committee {fec_committee_id: c.committee_id})
    MATCH (cand:Candidate {fec_candidate_id: c.candidate_id})
    CREATE (comm)-[:CONTRIBUTED_TO {
        amount: toFloat(c.transaction_amount),
        date: c.transaction_date,
        cycle: c.cycle
    }]->(cand)
    """
    return _batch_load(session, query, contributions)


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

    Args:
        ratings: List of dicts with keys: org_name, year, fec_candidate_id, score.
    """
    query = """
    UNWIND $batch AS r
    MATCH (sc:Scorecard {org_name: r.org_name, year: r.year})
    MATCH (cand:Candidate {fec_candidate_id: r.fec_candidate_id})
    MERGE (sc)-[rate:RATES]->(cand)
    SET rate.score = toFloat(r.score),
        rate.year = r.year
    """
    return _batch_load(session, query, ratings)


def _batch_load(session: Session, query: str, records: list[dict]) -> int:
    """Execute a batched UNWIND load query."""
    total = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        session.run(query, batch=batch)
        total += len(batch)
    logger.info("Loaded %d records", total)
    return total
