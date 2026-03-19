"""Score lookup endpoints for the browser extension."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import AsyncDriver

from api.deps import get_driver
from api.queries.scores import query_brand_scores, search_brand_scores

router = APIRouter(tags=["scores"])


@router.get("/scores/{brand_name}")
async def get_brand_scores(
    brand_name: str,
    driver: AsyncDriver = Depends(get_driver),
    issues: list[str] | None = Query(default=None, description="Filter by issue names"),
    scorecards: list[str] | None = Query(default=None, description="Filter by scorecard orgs"),
):
    """Get pre-computed issue scores for a brand."""
    async with driver.session() as session:
        scores = await query_brand_scores(session, brand_name, issues=issues, scorecards=scorecards)

    if not scores:
        raise HTTPException(status_code=404, detail=f"No scores found for brand: {brand_name}")

    return {"brand": brand_name, **scores}


@router.get("/scores")
async def search_scores(
    driver: AsyncDriver = Depends(get_driver),
    q: str = Query(description="Brand name search query"),
    issues: list[str] | None = Query(default=None, description="Filter by issue names"),
    scorecards: list[str] | None = Query(default=None, description="Filter by scorecard orgs"),
):
    """Search scores by brand name (substring match)."""
    async with driver.session() as session:
        results = await search_brand_scores(session, q, issues=issues, scorecards=scorecards)

    return {"results": results, "count": len(results)}
