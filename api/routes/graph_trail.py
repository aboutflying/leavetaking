"""Graph trail query endpoints for the detail view visualization."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["graph"])

# Full money trail query: brand -> corp -> PAC -> candidate -> issue
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

# Executive donations trail
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

# Graph nodes/edges for D3 visualization
GRAPH_VIZ_QUERY = """
MATCH path = (b:Brand {name: $brand_name})-[:OWNED_BY]->(c:Corporation)
OPTIONAL MATCH sub_path = (c)-[:SUBSIDIARY_OF*0..5]->(parent:Corporation)
OPTIONAL MATCH (parent)-[:OPERATES_PAC]->(comm:Committee)
OPTIONAL MATCH (comm)-[contrib:CONTRIBUTED_TO]->(cand:Candidate)
RETURN b, c, parent, comm, cand, contrib,
       labels(b) AS b_labels, labels(c) AS c_labels
LIMIT 200
"""


@router.get("/trail/{brand_name}")
async def get_money_trail(
    request: Request,
    brand_name: str,
    issues: list[str] | None = Query(default=None),
):
    """Get the full money trail for a brand.

    Returns PAC contribution paths and executive donation paths.
    """
    driver = request.app.state.neo4j_driver

    with driver.session() as session:
        # PAC trail
        pac_results = session.run(TRAIL_QUERY, brand_name=brand_name, issues=issues)
        pac_trail = [dict(record) for record in pac_results]

        # Executive trail
        exec_results = session.run(EXEC_TRAIL_QUERY, brand_name=brand_name, issues=issues)
        exec_trail = [dict(record) for record in exec_results]

    if not pac_trail and not exec_trail:
        raise HTTPException(status_code=404, detail=f"No trail found for brand: {brand_name}")

    return {
        "brand": brand_name,
        "pac_trail": pac_trail,
        "executive_trail": exec_trail,
        "total_pac_records": len(pac_trail),
        "total_exec_records": len(exec_trail),
    }


@router.get("/graph/{brand_name}")
async def get_graph_data(request: Request, brand_name: str):
    """Get graph nodes and edges for D3/vis.js visualization.

    Returns nodes and links in a format suitable for force-directed graph rendering.
    """
    driver = request.app.state.neo4j_driver

    nodes = {}
    links = []

    with driver.session() as session:
        results = session.run(GRAPH_VIZ_QUERY, brand_name=brand_name)

        for record in results:
            # Add brand node
            if record["b"]:
                b = record["b"]
                nodes[f"brand:{b['name']}"] = {
                    "id": f"brand:{b['name']}",
                    "label": b["name"],
                    "type": "brand",
                }

            # Add corporation nodes
            for key in ("c", "parent"):
                node = record[key]
                if node:
                    nid = f"corp:{node['name']}"
                    nodes[nid] = {
                        "id": nid,
                        "label": node["name"],
                        "type": "corporation",
                        "ticker": node.get("ticker"),
                    }

            # Add committee node
            if record["comm"]:
                comm = record["comm"]
                nid = f"comm:{comm['fec_committee_id']}"
                nodes[nid] = {
                    "id": nid,
                    "label": comm["name"],
                    "type": "committee",
                }

            # Add candidate node
            if record["cand"]:
                cand = record["cand"]
                nid = f"cand:{cand['fec_candidate_id']}"
                nodes[nid] = {
                    "id": nid,
                    "label": cand["name"],
                    "type": "candidate",
                    "party": cand.get("party"),
                }

    # Build links from relationships
    # (simplified — in production, extract from path traversal)
    if not nodes:
        raise HTTPException(status_code=404, detail=f"No graph data for brand: {brand_name}")

    return {
        "nodes": list(nodes.values()),
        "links": links,
    }
