"""Graph trail query endpoints for the detail view visualization."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import AsyncDriver

from api.deps import get_driver
from api.queries.graph_trail import query_exec_trail, query_graph_viz, query_pac_trail

router = APIRouter(tags=["graph"])


@router.get("/trail/{brand_name}")
async def get_money_trail(
    brand_name: str,
    driver: AsyncDriver = Depends(get_driver),
    issues: list[str] | None = Query(default=None),
):
    """Get the full money trail for a brand.

    Returns PAC contribution paths and executive donation paths.
    """
    async with driver.session() as session:
        pac_trail = await query_pac_trail(session, brand_name, issues=issues)
        exec_trail = await query_exec_trail(session, brand_name, issues=issues)

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
async def get_graph_data(
    brand_name: str,
    driver: AsyncDriver = Depends(get_driver),
):
    """Get graph nodes and edges for D3/vis.js visualization.

    Returns nodes and links in a format suitable for force-directed graph rendering.
    """
    async with driver.session() as session:
        nodes, links = await query_graph_viz(session, brand_name)

    if not nodes:
        raise HTTPException(status_code=404, detail=f"No graph data for brand: {brand_name}")

    return {"nodes": nodes, "links": links}
