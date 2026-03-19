"""Fetch and parse legislative scorecards from advocacy organizations."""

from __future__ import annotations

import csv
import json
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
    party: str | None = None  # single uppercase letter: R, D, I, etc.


# ---------------------------------------------------------------------------
# Grade normalization
# ---------------------------------------------------------------------------

GRADE_TO_SCORE: dict[str, float] = {
    "A+": 100.0,
    "A": 95.0,
    "A-": 90.0,
    "B+": 85.0,
    "B": 75.0,
    "B-": 70.0,
    "C+": 65.0,
    "C": 50.0,
    "C-": 45.0,
    "D+": 35.0,
    "D": 25.0,
    "D-": 20.0,
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

_SKIP_SCORE_VALUES_EXTENDED = _SKIP_SCORE_VALUES | {"na", "NA"}

# US state/territory name → 2-letter abbreviation, for moc-listing section headers
_STATE_ABBREVS: dict[str, str] = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC",
    "American Samoa": "AS", "Guam": "GU", "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR", "Virgin Islands": "VI",
}


class LCVFetcher:
    """Reads League of Conservation Voters annual scorecard from a local CSV.

    Supports two file formats:

    **moc-listing format** (preferred — LCV's actual download):
      Filename: ``data/scorecards/moc-listing-{year}-{date}.csv``
      Structure: Senate and House sections, state names as section dividers,
      columns: First Name, Last Name, Party, District, Year Score, Lifetime Score, URL

    **Legacy format** (fallback):
      Filename: ``data/scorecards/lcv_{year}.csv``
      Columns: Member, State, Party, {year} Score, Lifetime Score

    When both exist for the same year, moc-listing takes precedence.
    """

    def __init__(self, data_dir: Path, issue: str = "environment") -> None:
        self.data_dir = data_dir
        self.issue = issue

    def fetch(self, year: int) -> Iterator[RawRating]:
        moc_matches = sorted(self.data_dir.glob(f"moc-listing-{year}-*.csv"))
        if moc_matches:
            yield from self._fetch_moc_listing(moc_matches[-1], year)
            return
        path = self.data_dir / f"lcv_{year}.csv"
        if not path.exists():
            logger.info("LCV file not found for %d", year)
            return
        yield from self._fetch_legacy(path, year)

    def _fetch_moc_listing(self, path: Path, year: int) -> Iterator[RawRating]:
        """Parse LCV's moc-listing CSV format."""
        logger.info("Loading LCV moc-listing from %s", path)
        current_state: str | None = None

        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                stripped = [cell.strip() for cell in row]

                if len(stripped) == 1:
                    val = stripped[0]
                    if val in ("Senate", "House"):
                        current_state = None
                        continue
                    if val in _STATE_ABBREVS:
                        current_state = _STATE_ABBREVS[val]
                    continue

                if len(stripped) >= 7 and stripped[0] == "First Name":
                    continue  # header row

                if len(stripped) < 7:
                    continue

                first_name, last_name, party_raw, _district, score_raw = stripped[:5]
                candidate_name = f"{first_name} {last_name}".strip()

                if not candidate_name or not current_state:
                    logger.warning(
                        "Skipping row with missing name or state: %r", stripped
                    )
                    continue

                if score_raw in _SKIP_SCORE_VALUES_EXTENDED:
                    logger.debug("Skipping blank/NA score for %s", candidate_name)
                    continue

                try:
                    score = normalize_score(score_raw)
                except ValueError:
                    logger.warning(
                        "Skipping unrecognized score %r for %s", score_raw, candidate_name
                    )
                    continue

                yield RawRating(
                    org_name="League of Conservation Voters",
                    year=year,
                    issue=self.issue,
                    candidate_name=candidate_name,
                    state=current_state,
                    score=score,
                    party=party_raw.strip().upper()[:1] or None,
                )

    def _fetch_legacy(self, path: Path, year: int) -> Iterator[RawRating]:
        """Parse legacy lcv_{year}.csv format."""
        score_col = f"{year} Score"

        with open(path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if score_col not in (reader.fieldnames or []):
                logger.warning(
                    "LCV CSV for %d missing expected score column '%s' — skipping",
                    year,
                    score_col,
                )
                return

            for row in reader:
                member = row.get("Member", "").strip()
                state = row.get("State", "").strip().upper()
                party_raw = row.get("Party", "").strip().upper()
                score_raw = row.get(score_col, "").strip()

                if not member or not state:
                    logger.warning("Skipping row with missing member or state: %r", dict(row))
                    continue

                if score_raw in _SKIP_SCORE_VALUES_EXTENDED:
                    logger.debug("Skipping blank/NA score for %s", member)
                    continue

                try:
                    score = normalize_score(score_raw)
                except ValueError:
                    logger.warning("Skipping unrecognized score %r for %s", score_raw, member)
                    continue

                yield RawRating(
                    org_name="League of Conservation Voters",
                    year=year,
                    issue=self.issue,
                    candidate_name=member,
                    state=state,
                    score=score,
                    party=party_raw[:1] or None,
                )


# ---------------------------------------------------------------------------
# JsonFileFetcher (generic — for manually-maintained scorecard JSON files)
# ---------------------------------------------------------------------------


class JsonFileFetcher:
    """Reads a manual-download scorecard JSON for one org.

    Expected file: data/scorecards/{org_name_snake}_{year}.json
    where org_name_snake = org_name.lower().replace(' ', '_')
    e.g. 'ACLU' -> 'aclu_2024.json', 'EFF' -> 'eff_2024.json'

    JSON format::

        {
          "org_name": "ACLU",
          "year": 2024,
          "issue": "civil_liberties",
          "ratings": [
            {"candidate_name": "Nancy Pelosi", "state": "CA", "score": 95},
            {"candidate_name": "Ted Cruz", "state": "TX", "score": "F"}
          ]
        }

    Score values accept float, int, or letter grade string (A+/A/.../F).
    Blank/NA scores ("", "-", "N/A", "n/v", "NV") are silently skipped.
    """

    def __init__(self, data_dir: Path, org_name: str, issue: str) -> None:
        self.data_dir = data_dir
        self.org_name = org_name
        self.issue = issue

    def fetch(self, year: int) -> Iterator[RawRating]:
        slug = self.org_name.lower().replace(" ", "_")
        path = self.data_dir / f"{slug}_{year}.json"
        if not path.exists():
            logger.info("Scorecard file not found for %d: %s", year, path)
            return

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.error("Malformed JSON in scorecard file %s — skipping", path)
            return

        for r in data.get("ratings", []):
            state = r.get("state", "").strip().upper()
            candidate_name = r.get("candidate_name", "").strip()
            if not candidate_name or not state:
                logger.warning("Skipping rating missing candidate_name or state: %r", r)
                continue

            raw_val = r.get("score", "")
            if isinstance(raw_val, str) and raw_val.strip() in _SKIP_SCORE_VALUES:
                logger.debug("Skipping blank/NA score for %s", candidate_name)
                continue

            try:
                score = normalize_score(raw_val)
            except ValueError:
                logger.warning("Skipping unrecognized score %r for %s", raw_val, candidate_name)
                continue

            yield RawRating(
                org_name=self.org_name,
                year=data.get("year", year),
                issue=data.get("issue", self.issue),
                candidate_name=candidate_name,
                state=state,
                score=score,
            )


# ---------------------------------------------------------------------------
# Registry and orchestrator
# ---------------------------------------------------------------------------

FETCHER_REGISTRY: dict[str, ScorecardFetcher] = {
    "League of Conservation Voters": LCVFetcher(settings.data_dir / "scorecards"),
}


def load_all_scorecards(years: int | list[int]) -> Iterator[RawRating]:
    """Yield RawRating records from all registered fetchers for the given year(s).

    Accepts a single year int or a list of years. Adding a new org: register a
    fetcher in FETCHER_REGISTRY — no other changes needed.
    """
    if isinstance(years, int):
        years = [years]
    for org_name, fetcher in FETCHER_REGISTRY.items():
        for year in years:
            logger.info("Loading scorecard: %s %d", org_name, year)
            yield from fetcher.fetch(year)
