"""Score lookup endpoints for the browser extension."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from pipeline.processors.score_computation import (
    query_brand_scores_from_graph,
    search_brand_scores_from_graph,
)

router = APIRouter(tags=["scores"])


@router.get("/scores/{brand_name}")
async def get_brand_scores(
    request: Request,
    brand_name: str,
    issues: list[str] | None = Query(default=None, description="Filter by issue names"),
    scorecards: list[str] | None = Query(default=None, description="Filter by scorecard orgs"),
):
    """Get pre-computed issue scores for a brand."""
    driver = request.app.state.neo4j_driver
    with driver.session() as session:
        scores = query_brand_scores_from_graph(session, brand_name, issues=issues, scorecards=scorecards)

    if not scores:
        raise HTTPException(status_code=404, detail=f"No scores found for brand: {brand_name}")

    return {"brand": brand_name, **scores}


@router.get("/scores")
async def search_scores(
    request: Request,
    q: str = Query(description="Brand name search query"),
    issues: list[str] | None = Query(default=None, description="Filter by issue names"),
    scorecards: list[str] | None = Query(default=None, description="Filter by scorecard orgs"),
):
    """Search scores by brand name (substring match)."""
    driver = request.app.state.neo4j_driver
    with driver.session() as session:
        results = search_brand_scores_from_graph(session, q, issues=issues, scorecards=scorecards)

    return {"results": results, "count": len(results)}
