"""Fetch and parse legislative scorecards from advocacy organizations."""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from pipeline.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class RawRating:
    """A single candidate rating from a scorecard, before FEC ID resolution."""

    org_name: str
    year: int
    issue: str
    candidate_name: str
    state: str
    score: float


# ---------------------------------------------------------------------------
# Grade normalization
# ---------------------------------------------------------------------------

GRADE_TO_SCORE: dict[str, float] = {
    "A+": 100.0, "A": 95.0, "A-": 90.0,
    "B+": 85.0,  "B": 75.0, "B-": 70.0,
    "C+": 65.0,  "C": 50.0, "C-": 45.0,
    "D+": 35.0,  "D": 25.0, "D-": 20.0,
    "F": 0.0,
}

# Score cell values that mean "did not vote / not applicable" — skip silently
_SKIP_SCORE_VALUES = {"", "-", "N/A", "n/v", "NV"}


def normalize_score(raw: str | float | int) -> float:
    """Convert a raw score value to a 0–100 float.

    Accepts:
    - Numeric types (int, float) — returned as float directly
    - Numeric strings ('73.5', '100') — parsed as float
    - Letter grade strings ('A+', 'B-', 'F') — converted via GRADE_TO_SCORE

    Raises:
        ValueError: For any string not recognized as numeric or a valid grade.
    """
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped in GRADE_TO_SCORE:
            return GRADE_TO_SCORE[stripped]
        try:
            return float(stripped)
        except ValueError:
            pass
    raise ValueError(f"Unknown score value: {raw!r}")


# ---------------------------------------------------------------------------
# Fetcher protocol
# ---------------------------------------------------------------------------

class ScorecardFetcher(Protocol):
    """Protocol for per-org scorecard fetchers."""

    def fetch(self, year: int) -> Iterator[RawRating]:
        """Yield RawRating records for the given election cycle year."""
        ...


# ---------------------------------------------------------------------------
# LCV fetcher (reference implementation)
# ---------------------------------------------------------------------------

class LCVFetcher:
    """Reads League of Conservation Voters annual scorecard from a local CSV.

    Expected file: data/scorecards/lcv_{year}.csv
    Downloaded manually from scorecard.lcv.org (no public API available).

    CSV columns: Member, State, Party, {year} Score, Lifetime Score
    """

    def __init__(self, data_dir: Path, issue: str = "environment") -> None:
        self.data_dir = data_dir
        self.issue = issue

    def fetch(self, year: int) -> Iterator[RawRating]:
        path = self.data_dir / f"lcv_{year}.csv"
        if not path.exists():
            logger.info("LCV file not found for %d: %s", year, path)
            return

        # encoding='utf-8-sig' strips BOM from Excel-exported CSVs
        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []

            # Detect year-specific score column (e.g. '2024 Score')
            target = f"{year} score"
            score_col = next(
                (fn for fn in fieldnames if fn.strip().lower() == target),
                None,
            )
            if score_col is None:
                logger.warning(
                    "LCV CSV %s has no '%d Score' column (found: %s)",
                    path.name, year, fieldnames,
                )
                return

            for row in reader:
                score_raw = row.get(score_col, "").strip()

                if score_raw in _SKIP_SCORE_VALUES:
                    logger.debug(
                        "Skipping blank/NA score for %s", row.get("Member")
                    )
                    continue

                try:
                    score = normalize_score(score_raw)
                except ValueError:
                    logger.warning(
                        "Skipping unrecognized score %r for %s",
                        score_raw, row.get("Member"),
                    )
                    continue

                candidate_name = row.get("Member", "").strip()
                state = row.get("State", "").strip().upper()

                if not candidate_name or not state:
                    logger.warning(
                        "Skipping row with missing Member or State: %r", row
                    )
                    continue

                yield RawRating(
                    org_name="League of Conservation Voters",
                    year=year,
                    issue=self.issue,
                    candidate_name=candidate_name,
                    state=state,
                    score=score,
                )


# ---------------------------------------------------------------------------
# Registry and orchestrator
# ---------------------------------------------------------------------------

FETCHER_REGISTRY: dict[str, ScorecardFetcher] = {
    "League of Conservation Voters": LCVFetcher(
        settings.data_dir / "scorecards"
    ),
}


def load_all_scorecards(cycles: list[int]) -> Iterator[RawRating]:
    """Yield RawRating records from all registered fetchers across all cycles.

    Adding a new org: register a fetcher in FETCHER_REGISTRY — no other
    changes needed here or in run_pipeline.py.
    """
    for org_name, fetcher in FETCHER_REGISTRY.items():
        for year in cycles:
            logger.info("Loading scorecard: %s %d", org_name, year)
            yield from fetcher.fetch(year)
