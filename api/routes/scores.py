"""Score lookup endpoints for the browser extension."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

from pipeline.config import settings

router = APIRouter(tags=["scores"])

_scores_cache: dict | None = None


def _load_scores() -> dict:
    """Load pre-computed scores from the JSON file."""
    global _scores_cache
    if _scores_cache is not None:
        return _scores_cache

    path = settings.scores_output
    if not path.exists():
        return {"meta": {"version": "0.2", "brand_count": 0}, "brands": {}}

    _scores_cache = json.loads(path.read_text())
    return _scores_cache


@router.get("/scores/{brand_name}")
async def get_brand_scores(brand_name: str):
    """Get pre-computed issue scores for a brand."""
    scores = _load_scores()
    brand_data = scores.get("brands", {}).get(brand_name)

    if brand_data is None:
        raise HTTPException(status_code=404, detail=f"No scores found for brand: {brand_name}")

    return {"brand": brand_name, **brand_data}


@router.get("/scores")
async def search_scores(
    q: str = Query(description="Brand name search query"),
    issues: list[str] | None = Query(default=None, description="Filter by issue names"),
):
    """Search scores by brand name (prefix match)."""
    scores = _load_scores()
    q_lower = q.lower()

    results = []
    for brand_name, brand_data in scores.get("brands", {}).items():
        if q_lower in brand_name.lower():
            entry = {"brand": brand_name}
            for issue, data in brand_data.items():
                if not issues or issue in issues:
                    entry[issue] = data
            results.append(entry)

    return {"results": results, "count": len(results)}


@router.post("/scores/reload")
async def reload_scores():
    """Force reload of the pre-computed scores cache."""
    global _scores_cache
    _scores_cache = None
    data = _load_scores()
    return {"status": "reloaded", "brand_count": data.get("meta", {}).get("brand_count", 0)}
