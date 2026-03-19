"""Pre-compute per-brand, per-issue scores by traversing the graph."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from neo4j import Session

from pipeline.config import settings

logger = logging.getLogger(__name__)

# Single UNION ALL query combining PAC and executive donation paths.
# Returns one row per (brand → corp → candidate → scorecard → issue) traversal.
# Python aggregates by (issue, scorecard) after the fact.
CONTRIBUTION_QUERY = """
MATCH (b:Brand {name: $brand_name})-[:OWNED_BY]->(:Corporation)
      -[:SUBSIDIARY_OF*0..10]->(corp:Corporation)
MATCH (corp)-[:OPERATES_PAC]->(:Committee)-[c:CONTRIBUTED_TO]->(cand:Candidate)
WHERE c.cycle IN $cycles
MATCH (sc:Scorecard)-[r:RATES]->(cand)
MATCH (sc)-[:COVERS]->(issue:Issue)
RETURN issue.name AS issue,
       sc.org_name AS scorecard,
       cand.fec_candidate_id AS candidate_id,
       r.score AS score,
       toFloat(c.amount) AS dollars
UNION ALL
MATCH (b:Brand {name: $brand_name})-[:OWNED_BY]->(:Corporation)
      -[:SUBSIDIARY_OF*0..10]->(corp:Corporation)
MATCH (p:Person)-[:EXECUTIVE_OF]->(corp)
MATCH (p)-[d:DONATED_TO]->(cand:Candidate)
WHERE d.cycle IN $cycles
MATCH (sc:Scorecard)-[r:RATES]->(cand)
MATCH (sc)-[:COVERS]->(issue:Issue)
RETURN issue.name AS issue,
       sc.org_name AS scorecard,
       cand.fec_candidate_id AS candidate_id,
       r.score AS score,
       toFloat(d.amount) AS dollars
"""

ALL_BRANDS_QUERY = "MATCH (b:Brand) RETURN b.name AS name"


def _weighted_score(rows: list[dict]) -> float | None:
    """Compute dollar-weighted average score from a list of contribution rows.

    Falls back to unweighted average when all dollars are zero.
    Skips rows where score is None with a warning.
    Returns None if no rows have a valid score.
    """
    valid = [r for r in rows if r["score"] is not None]
    if not valid:
        return None

    skipped = len(rows) - len(valid)
    if skipped:
        logger.warning("Skipping %d rows with None score", skipped)

    total_dollars = sum(r["dollars"] for r in valid if r["dollars"])
    if total_dollars == 0:
        # Zero-dollar fallback: unweighted average
        return sum(r["score"] for r in valid) / len(valid)

    return sum(r["score"] * r["dollars"] for r in valid if r["dollars"]) / sum(
        r["dollars"] for r in valid if r["dollars"]
    )


def _confidence(total_dollars: float, candidate_count: int) -> str:
    """Return confidence tier based on dollar flow and unique candidate count."""
    if total_dollars > 100_000 and candidate_count >= 5:
        return "high"
    if total_dollars > 10_000 or candidate_count >= 2:
        return "medium"
    return "low"


def compute_brand_scores(session: Session, brand_name: str, cycles: list[int]) -> dict:
    """Compute per-issue, per-scorecard scores for a single brand.

    Returns nested dict: issue → scorecard_org → {score, dollars, candidates, confidence}
    Returns {} if the brand has no ownership edges or no matching contributions.
    Exceptions from session.run() propagate (do not swallow DB errors).
    """
    rows = list(session.run(CONTRIBUTION_QUERY, brand_name=brand_name, cycles=cycles))
    if not rows:
        return {}

    # Group by (issue, scorecard)
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        key = (row["issue"], row["scorecard"])
        groups[key].append(
            {
                "candidate_id": row["candidate_id"],
                "score": row["score"],
                "dollars": row["dollars"] or 0.0,
            }
        )

    result: dict[str, dict[str, dict]] = defaultdict(dict)
    for (issue, scorecard), group_rows in groups.items():
        score = _weighted_score(group_rows)
        if score is None:
            continue

        total_dollars = sum(r["dollars"] for r in group_rows)
        candidate_count = len({r["candidate_id"] for r in group_rows})
        # Confidence is 'low' when all dollars are zero (zero-dollar fallback path)
        all_zero = total_dollars == 0
        confidence = "low" if all_zero else _confidence(total_dollars, candidate_count)

        result[issue][scorecard] = {
            "score": round(score, 1),
            "dollars": total_dollars,
            "candidates": candidate_count,
            "confidence": confidence,
        }

    return dict(result)


def compute_all_scores(session: Session, cycles: list[int] | None = None) -> dict:
    """Compute scores for all brands in the graph.

    Returns dict mapping brand name to compute_brand_scores() output.
    Brands with no data are omitted from the result.
    """
    if cycles is None:
        cycles = settings.fec_cycles

    brands = session.run(ALL_BRANDS_QUERY)
    all_scores = {}

    for record in brands:
        brand_name = record["name"]
        logger.info("Computing scores for: %s", brand_name)
        brand_scores = compute_brand_scores(session, brand_name, cycles)
        if brand_scores:
            all_scores[brand_name] = brand_scores

    logger.info("Computed scores for %d brands", len(all_scores))
    return all_scores


def export_scores(scores: dict, output_path: Path | None = None) -> Path:
    """Export pre-computed scores to JSON for the browser extension.

    Output schema (v0.2):
    {
        "meta": {"version": "0.2", "generated_at": "...", "brand_count": N},
        "brands": {
            "BrandName": {
                "environment": {
                    "League of Conservation Voters": {
                        "score": 45.2,
                        "dollars": 125000,
                        "candidates": 8,
                        "confidence": "high"
                    }
                }
            }
        }
    }

    The extension combines scores across user-trusted scorecard orgs client-side.
    """
    if output_path is None:
        output_path = settings.scores_output

    export = {
        "meta": {
            "version": "0.2",
            "generated_at": datetime.now(UTC).isoformat(),
            "brand_count": len(scores),
        },
        "brands": scores,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(export, indent=2))
    logger.info("Exported scores to %s", output_path)
    return output_path
