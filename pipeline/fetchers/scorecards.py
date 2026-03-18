"""Fetch and parse legislative scorecards from advocacy organizations."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import requests

from pipeline.config import settings

logger = logging.getLogger(__name__)


# Scorecard data is heterogeneous — each org publishes in a different format.
# This module provides a unified interface with per-org parsers.


class ScorecardRecord:
    """A single candidate rating from a scorecard."""

    def __init__(
        self,
        org_name: str,
        year: int,
        candidate_name: str,
        fec_candidate_id: str | None,
        score: float,
        issue: str,
    ):
        self.org_name = org_name
        self.year = year
        self.candidate_name = candidate_name
        self.fec_candidate_id = fec_candidate_id
        self.score = score  # Normalized to 0-100
        self.issue = issue

    def to_dict(self) -> dict:
        return {
            "org_name": self.org_name,
            "year": self.year,
            "candidate_name": self.candidate_name,
            "fec_candidate_id": self.fec_candidate_id,
            "score": self.score,
            "issue": self.issue,
        }


def fetch_lcv_scorecard(year: int = 2024) -> list[ScorecardRecord]:
    """Fetch League of Conservation Voters National Environmental Scorecard.

    LCV publishes scores as percentages (0-100) for each member of Congress.
    """
    cache_path = settings.data_dir / "scorecards" / f"lcv_{year}.json"

    if cache_path.exists():
        data = json.loads(cache_path.read_text())
    else:
        # LCV scorecard API endpoint (public)
        url = f"https://scorecard.lcv.org/exports/{year}-scorecard.json"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(data))
        except requests.RequestException:
            logger.warning("Could not fetch LCV scorecard for %d, using empty", year)
            return []

    records = []
    for member in data if isinstance(data, list) else data.get("members", []):
        records.append(ScorecardRecord(
            org_name="League of Conservation Voters",
            year=year,
            candidate_name=member.get("name", ""),
            fec_candidate_id=member.get("fec_id"),
            score=float(member.get("score", 0)),
            issue="environment",
        ))
    return records


def load_scorecard_from_file(path: Path) -> list[ScorecardRecord]:
    """Load a manually curated scorecard JSON file.

    Expected format:
    {
        "org_name": "ACLU",
        "year": 2024,
        "issue": "civil_liberties",
        "ratings": [
            {"candidate_name": "...", "fec_candidate_id": "...", "score": 85},
            ...
        ]
    }
    """
    data = json.loads(path.read_text())
    org_name = data["org_name"]
    year = data["year"]
    issue = data["issue"]

    return [
        ScorecardRecord(
            org_name=org_name,
            year=year,
            candidate_name=r["candidate_name"],
            fec_candidate_id=r.get("fec_candidate_id"),
            score=float(r["score"]),
            issue=issue,
        )
        for r in data.get("ratings", [])
    ]


def load_all_manual_scorecards() -> list[ScorecardRecord]:
    """Load all manually curated scorecard files from the data directory."""
    scorecard_dir = settings.data_dir / "scorecards"
    if not scorecard_dir.exists():
        return []

    records = []
    for path in scorecard_dir.glob("*.json"):
        try:
            records.extend(load_scorecard_from_file(path))
            logger.info("Loaded %s", path.name)
        except (json.JSONDecodeError, KeyError):
            logger.exception("Failed to load scorecard: %s", path)
    return records
