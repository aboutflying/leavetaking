"""Pre-compute per-brand, per-issue scores by traversing the graph."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from neo4j import Session

from pipeline.config import settings

logger = logging.getLogger(__name__)

# Core query: PAC contributions path
PAC_SCORE_QUERY = """
MATCH (b:Brand {name: $brand_name})-[:OWNED_BY]->(:Corporation)-[:SUBSIDIARY_OF*0..10]->(corp:Corporation)
MATCH (corp)-[:OPERATES_PAC]->(:Committee)-[c:CONTRIBUTED_TO]->(cand:Candidate)
MATCH (sc:Scorecard)-[r:RATES]->(cand)
MATCH (sc)-[:COVERS]->(issue:Issue)
RETURN issue.name AS issue,
       AVG(r.score) AS avg_score,
       SUM(toFloat(c.amount)) AS total_pac_money,
       COUNT(DISTINCT cand) AS candidate_count
"""

# Executive individual donations path
EXEC_SCORE_QUERY = """
MATCH (b:Brand {name: $brand_name})-[:OWNED_BY]->(:Corporation)-[:SUBSIDIARY_OF*0..10]->(corp:Corporation)
MATCH (p:Person)-[:EXECUTIVE_OF]->(corp)
MATCH (p)-[d:DONATED_TO]->(cand:Candidate)
MATCH (sc:Scorecard)-[r:RATES]->(cand)
MATCH (sc)-[:COVERS]->(issue:Issue)
RETURN issue.name AS issue,
       AVG(r.score) AS avg_exec_score,
       SUM(toFloat(d.amount)) AS total_exec_money,
       COUNT(DISTINCT cand) AS candidate_count
"""

ALL_BRANDS_QUERY = "MATCH (b:Brand) RETURN b.name AS name"


def compute_brand_scores(session: Session, brand_name: str) -> dict:
    """Compute issue scores for a single brand.

    Returns:
        Dict mapping issue name to score details:
        {
            "environment": {
                "pac_score": 45.2,
                "exec_score": 38.7,
                "combined_score": 42.0,
                "pac_money": 150000,
                "exec_money": 25000,
                "candidate_count": 12,
                "confidence": "medium"
            },
            ...
        }
    """
    scores = {}

    # PAC path
    pac_results = session.run(PAC_SCORE_QUERY, brand_name=brand_name)
    for record in pac_results:
        issue = record["issue"]
        scores[issue] = {
            "pac_score": round(record["avg_score"], 1) if record["avg_score"] else None,
            "pac_money": record["total_pac_money"] or 0,
            "pac_candidates": record["candidate_count"] or 0,
        }

    # Executive path
    exec_results = session.run(EXEC_SCORE_QUERY, brand_name=brand_name)
    for record in exec_results:
        issue = record["issue"]
        if issue not in scores:
            scores[issue] = {"pac_score": None, "pac_money": 0, "pac_candidates": 0}
        scores[issue]["exec_score"] = (
            round(record["avg_exec_score"], 1) if record["avg_exec_score"] else None
        )
        scores[issue]["exec_money"] = record["total_exec_money"] or 0
        scores[issue]["exec_candidates"] = record["candidate_count"] or 0

    # Combine scores and compute confidence
    for issue, data in scores.items():
        pac = data.get("pac_score")
        exc = data.get("exec_score")

        if pac is not None and exc is not None:
            # Weight PAC score higher since it's more direct
            data["combined_score"] = round(pac * 0.6 + exc * 0.4, 1)
        elif pac is not None:
            data["combined_score"] = pac
        elif exc is not None:
            data["combined_score"] = exc
        else:
            data["combined_score"] = None

        # Confidence based on money flow and candidate count
        total_money = data.get("pac_money", 0) + data.get("exec_money", 0)
        total_candidates = data.get("pac_candidates", 0) + data.get("exec_candidates", 0)

        if total_money > 100_000 and total_candidates >= 5:
            data["confidence"] = "high"
        elif total_money > 10_000 or total_candidates >= 2:
            data["confidence"] = "medium"
        else:
            data["confidence"] = "low"

    return scores


def compute_all_scores(session: Session) -> dict:
    """Compute scores for all brands in the graph.

    Returns:
        Dict mapping brand name to issue scores.
    """
    brands = session.run(ALL_BRANDS_QUERY)
    all_scores = {}

    for record in brands:
        brand_name = record["name"]
        logger.info("Computing scores for: %s", brand_name)
        brand_scores = compute_brand_scores(session, brand_name)
        if brand_scores:
            all_scores[brand_name] = brand_scores

    logger.info("Computed scores for %d brands", len(all_scores))
    return all_scores


def export_scores(scores: dict, output_path: Path | None = None) -> Path:
    """Export pre-computed scores to JSON for the browser extension.

    The output format is optimized for fast lookup by brand name:
    {
        "meta": {"version": "0.1", "brand_count": 500},
        "brands": {
            "BrandName": {
                "issues": {
                    "environment": {"score": 45.2, "confidence": "medium"},
                    ...
                }
            }
        }
    }
    """
    if output_path is None:
        output_path = settings.scores_output

    # Flatten to extension-friendly format
    export = {
        "meta": {"version": "0.1", "brand_count": len(scores)},
        "brands": {},
    }

    for brand_name, issue_scores in scores.items():
        export["brands"][brand_name] = {
            "issues": {
                issue: {
                    "score": data.get("combined_score"),
                    "confidence": data.get("confidence", "low"),
                }
                for issue, data in issue_scores.items()
                if data.get("combined_score") is not None
            }
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(export, indent=2))
    logger.info("Exported scores to %s", output_path)
    return output_path
