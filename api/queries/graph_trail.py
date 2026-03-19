"""Async Neo4j query functions for money trail and graph visualization."""

from __future__ import annotations

from neo4j import AsyncSession

# Full money trail: brand → corp → PAC → candidate → issue scores
TRAIL_QUERY = """
MATCH path = (b:Brand {name: $brand_name})-[:OWNED_BY]->(:Corporation)-[:SUBSIDIARY_OF*0..10]->(corp:Corporation)
MATCH (corp)-[:OPERATES_PAC]->(comm:Committee)-[c:CONTRIBUTED_TO]->(cand:Candidate)
MATCH (sc:Scorecard)-[r:RATES]->(cand)
MATCH (sc)-[:COVERS]->(issue:Issue)
WHERE ($issues IS NULL OR issue.name IN $issues)
RETURN b.name AS brand,
       corp.name AS corporation,
       comm.name AS committee,
       comm.fec_committee_id AS committee_id,
       c.amount AS contribution_amount,
       c.date AS contribution_date,
       cand.name AS candidate,
       cand.fec_candidate_id AS candidate_id,
       cand.party AS party,
       r.score AS scorecard_score,
       sc.org_name AS scorecard_org,
       issue.name AS issue
ORDER BY c.amount DESC
LIMIT 100
"""

# Executive donation trail
EXEC_TRAIL_QUERY = """
MATCH (b:Brand {name: $brand_name})-[:OWNED_BY]->(:Corporation)-[:SUBSIDIARY_OF*0..10]->(corp:Corporation)
MATCH (p:Person)-[:EXECUTIVE_OF]->(corp)
MATCH (p)-[d:DONATED_TO]->(cand:Candidate)
MATCH (sc:Scorecard)-[r:RATES]->(cand)
MATCH (sc)-[:COVERS]->(issue:Issue)
WHERE ($issues IS NULL OR issue.name IN $issues)
RETURN b.name AS brand,
       corp.name AS corporation,
       p.name AS executive,
       p.title AS executive_title,
       d.amount AS donation_amount,
       d.date AS donation_date,
       cand.name AS candidate,
       cand.fec_candidate_id AS candidate_id,
       cand.party AS party,
       r.score AS scorecard_score,
       sc.org_name AS scorecard_org,
       issue.name AS issue
ORDER BY d.amount DESC
LIMIT 100
"""

# Graph visualization: returns scalar fields so links can be built in Python.
# anc covers brand's direct corp and all subsidiaries up to 5 hops (*0..5).
GRAPH_VIZ_QUERY = """
MATCH (b:Brand {name: $brand_name})-[:OWNED_BY]->(c:Corporation)
OPTIONAL MATCH (c)-[:SUBSIDIARY_OF*0..5]->(anc:Corporation)
WITH b, c, anc
OPTIONAL MATCH (anc)-[:OPERATES_PAC]->(comm:Committee)
OPTIONAL MATCH (comm)-[:CONTRIBUTED_TO]->(cand:Candidate)
RETURN DISTINCT
  b.name AS brand_name,
  c.name AS corp_name, c.ticker AS corp_ticker,
  anc.name AS anc_name, anc.ticker AS anc_ticker,
  comm.name AS comm_name, comm.fec_committee_id AS comm_id,
  cand.name AS cand_name, cand.fec_candidate_id AS cand_id, cand.party AS cand_party
LIMIT 200
"""


async def query_pac_trail(
    session: AsyncSession,
    brand_name: str,
    issues: list[str] | None = None,
) -> list[dict]:
    result = await session.run(TRAIL_QUERY, brand_name=brand_name, issues=issues)
    return await result.data()


async def query_exec_trail(
    session: AsyncSession,
    brand_name: str,
    issues: list[str] | None = None,
) -> list[dict]:
    result = await session.run(EXEC_TRAIL_QUERY, brand_name=brand_name, issues=issues)
    return await result.data()


async def query_graph_viz(
    session: AsyncSession,
    brand_name: str,
) -> tuple[list[dict], list[dict]]:
    """Return (nodes, links) for D3/vis.js visualization.

    Nodes: [{id, label, type, ...}]
    Links: [{source, target, type}]  — deduplicated
    """
    result = await session.run(GRAPH_VIZ_QUERY, brand_name=brand_name)
    records = await result.data()

    nodes: dict[str, dict] = {}
    seen_links: set[tuple[str, str, str]] = set()
    links: list[dict] = []

    def add_link(source: str, target: str, rel_type: str) -> None:
        key = (source, target, rel_type)
        if key not in seen_links:
            seen_links.add(key)
            links.append({"source": source, "target": target, "type": rel_type})

    for r in records:
        brand_id = f"brand:{r['brand_name']}"
        corp_id = f"corp:{r['corp_name']}"

        # Brand node
        if r["brand_name"] and brand_id not in nodes:
            nodes[brand_id] = {"id": brand_id, "label": r["brand_name"], "type": "brand"}

        # Direct corporation node
        if r["corp_name"] and corp_id not in nodes:
            nodes[corp_id] = {
                "id": corp_id,
                "label": r["corp_name"],
                "type": "corporation",
                "ticker": r["corp_ticker"],
            }

        # Ancestor corporation node (may equal corp_id at hop 0)
        anc_id = f"corp:{r['anc_name']}" if r["anc_name"] else None
        if r["anc_name"] and anc_id not in nodes:
            nodes[anc_id] = {
                "id": anc_id,
                "label": r["anc_name"],
                "type": "corporation",
                "ticker": r["anc_ticker"],
            }

        # Committee node
        comm_id = f"comm:{r['comm_id']}" if r["comm_id"] else None
        if r["comm_id"] and comm_id not in nodes:
            nodes[comm_id] = {
                "id": comm_id,
                "label": r["comm_name"],
                "type": "committee",
            }

        # Candidate node
        cand_id = f"cand:{r['cand_id']}" if r["cand_id"] else None
        if r["cand_id"] and cand_id not in nodes:
            nodes[cand_id] = {
                "id": cand_id,
                "label": r["cand_name"],
                "type": "candidate",
                "party": r["cand_party"],
            }

        # Links
        if r["brand_name"] and r["corp_name"]:
            add_link(brand_id, corp_id, "OWNED_BY")
        if r["anc_name"] and r["anc_name"] != r["corp_name"]:
            add_link(corp_id, anc_id, "SUBSIDIARY_OF")
        if anc_id and r["comm_id"]:
            add_link(anc_id, comm_id, "OPERATES_PAC")
        elif r["corp_name"] and r["comm_id"]:
            add_link(corp_id, comm_id, "OPERATES_PAC")
        if r["comm_id"] and r["cand_id"]:
            add_link(comm_id, cand_id, "CONTRIBUTED_TO")

    return list(nodes.values()), links
