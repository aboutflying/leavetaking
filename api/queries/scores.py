"""Async Neo4j query functions for brand score lookups."""

from __future__ import annotations

from neo4j import AsyncSession

BRAND_SCORES_QUERY = """
MATCH (b:Brand {name: $brand_name})-[:HAS_SCORE]->(s:BrandScore)
      -[:FOR_ISSUE]->(i:Issue)
MATCH (s)-[:VIA_SCORECARD]->(sc:Scorecard)
WHERE ($issues IS NULL OR i.name IN $issues)
  AND ($scorecards IS NULL OR sc.org_name IN $scorecards)
RETURN i.name        AS issue,
       sc.org_name   AS scorecard,
       s.score       AS score,
       s.dollars     AS dollars,
       s.candidates  AS candidates,
       s.confidence  AS confidence,
       s.cycles      AS cycles,
       s.computed_at AS computed_at
ORDER BY i.name, sc.org_name
"""

SEARCH_BRAND_SCORES_QUERY = """
MATCH (b:Brand)-[:HAS_SCORE]->(s:BrandScore)-[:FOR_ISSUE]->(i:Issue)
MATCH (s)-[:VIA_SCORECARD]->(sc:Scorecard)
WHERE toLower(b.name) CONTAINS toLower($q)
  AND ($issues IS NULL OR i.name IN $issues)
  AND ($scorecards IS NULL OR sc.org_name IN $scorecards)
RETURN b.name        AS brand_name,
       i.name        AS issue,
       sc.org_name   AS scorecard,
       s.score       AS score,
       s.dollars     AS dollars,
       s.candidates  AS candidates,
       s.confidence  AS confidence,
       s.cycles      AS cycles
ORDER BY b.name, i.name, sc.org_name
"""


async def query_brand_scores(
    session: AsyncSession,
    brand_name: str,
    issues: list[str] | None = None,
    scorecards: list[str] | None = None,
) -> dict:
    """Return nested issue→scorecard→metrics dict for a single brand."""
    result = await session.run(
        BRAND_SCORES_QUERY,
        brand_name=brand_name,
        issues=issues,
        scorecards=scorecards,
    )
    records = await result.data()

    out: dict[str, dict[str, dict]] = {}
    for r in records:
        issue = r["issue"]
        scorecard = r["scorecard"]
        if issue not in out:
            out[issue] = {}
        out[issue][scorecard] = {
            "score": r["score"],
            "dollars": r["dollars"],
            "candidates": r["candidates"],
            "confidence": r["confidence"],
            "cycles": r["cycles"],
            "computed_at": str(r["computed_at"]) if r["computed_at"] else None,
        }
    return out


async def search_brand_scores(
    session: AsyncSession,
    q: str,
    issues: list[str] | None = None,
    scorecards: list[str] | None = None,
) -> list[dict]:
    """Return list of per-brand score dicts matching a name substring."""
    result = await session.run(
        SEARCH_BRAND_SCORES_QUERY,
        q=q,
        issues=issues,
        scorecards=scorecards,
    )
    records = await result.data()

    brands: dict[str, dict] = {}
    for r in records:
        brand_name = r["brand_name"]
        if brand_name not in brands:
            brands[brand_name] = {"brand": brand_name}
        issue = r["issue"]
        scorecard = r["scorecard"]
        if issue not in brands[brand_name]:
            brands[brand_name][issue] = {}
        brands[brand_name][issue][scorecard] = {
            "score": r["score"],
            "dollars": r["dollars"],
            "candidates": r["candidates"],
            "confidence": r["confidence"],
            "cycles": r["cycles"],
        }
    return list(brands.values())
