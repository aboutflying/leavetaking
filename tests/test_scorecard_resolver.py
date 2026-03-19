"""Tests for pipeline/processors/scorecard_resolver.py."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from pipeline.fetchers.scorecards import RawRating
from pipeline.processors.scorecard_resolver import (
    build_candidate_index,
    normalize_fec_name,
    normalize_scorecard_name,
    resolve_candidates,
)


# ---------------------------------------------------------------------------
# normalize_fec_name
# ---------------------------------------------------------------------------


def test_normalize_fec_name_basic():
    assert normalize_fec_name("MENENDEZ, ROBERT") == "robert menendez"


def test_normalize_fec_name_suffix_stripped():
    """JR suffix is stripped from the first-name portion of FEC names."""
    assert normalize_fec_name("KENNEDY, ROBERT F JR") == "robert f kennedy"


def test_normalize_fec_name_sr_stripped():
    assert normalize_fec_name("BYRD, ROBERT C SR") == "robert c byrd"


def test_normalize_fec_name_no_comma():
    """No comma in name: return lowercased without crashing."""
    assert normalize_fec_name("SMITH") == "smith"


def test_normalize_fec_name_multiple_suffixes_stripped():
    """Only the outermost suffix token is stripped (JR at end)."""
    # Edge case: name with two suffix-like tokens at end — only last stripped
    result = normalize_fec_name("DOE, JOHN JR")
    assert result == "john doe"


# ---------------------------------------------------------------------------
# normalize_scorecard_name
# ---------------------------------------------------------------------------


def test_normalize_scorecard_name_basic():
    assert normalize_scorecard_name("Robert Menendez") == "robert menendez"


def test_normalize_scorecard_name_extra_whitespace():
    assert normalize_scorecard_name("  Nancy  Pelosi  ") == "nancy pelosi"


def test_normalize_scorecard_name_already_lower():
    assert normalize_scorecard_name("ted cruz") == "ted cruz"


# ---------------------------------------------------------------------------
# build_candidate_index
# ---------------------------------------------------------------------------


def _mock_session(records: list[dict]) -> MagicMock:
    """Return a MagicMock neo4j Session whose .run() returns the given records."""
    session = MagicMock()
    session.run.return_value = records
    return session


def test_build_candidate_index_structure():
    """Index maps (normalized_name, state) -> [fec_candidate_id]."""
    session = _mock_session(
        [
            {"fec_id": "S6NJ00289", "name": "MENENDEZ, ROBERT", "state": "NJ"},
        ]
    )
    result = build_candidate_index(session)
    assert result == {("robert menendez", "NJ"): ["S6NJ00289"]}


def test_build_candidate_index_fec_name_normalized():
    """FEC name is normalized via normalize_fec_name (reversed + lowercased)."""
    session = _mock_session(
        [
            {"fec_id": "H8CA05036", "name": "PELOSI, NANCY", "state": "CA"},
        ]
    )
    result = build_candidate_index(session)
    assert ("nancy pelosi", "CA") in result


def test_build_candidate_index_none_state():
    """Candidate with state=None is coerced to empty string — no crash."""
    session = _mock_session(
        [
            {"fec_id": "X123", "name": "SMITH, JOHN", "state": None},
        ]
    )
    result = build_candidate_index(session)
    assert ("john smith", "") in result
    assert result[("john smith", "")] == ["X123"]


def test_build_candidate_index_multiple_candidates_same_key():
    """Two candidates with identical normalized name+state both appear in list."""
    session = _mock_session(
        [
            {"fec_id": "X1", "name": "DOE, JANE", "state": "OH"},
            {"fec_id": "X2", "name": "DOE, JANE", "state": "OH"},
        ]
    )
    result = build_candidate_index(session)
    assert len(result[("jane doe", "OH")]) == 2


def test_build_candidate_index_empty():
    """Empty result set returns empty dict."""
    session = _mock_session([])
    result = build_candidate_index(session)
    assert result == {}


# ---------------------------------------------------------------------------
# resolve_candidates
# ---------------------------------------------------------------------------


def _make_rating(candidate_name: str, state: str, score: float = 75.0) -> RawRating:
    return RawRating(
        org_name="LCV",
        year=2024,
        issue="environment",
        candidate_name=candidate_name,
        state=state,
        score=score,
    )


def test_resolve_candidates_match():
    """Single match yields resolved dict with fec_candidate_id."""
    index = {("nancy pelosi", "CA"): ["H8CA05036"]}
    rating = _make_rating("Nancy Pelosi", "CA", 100.0)
    results = list(resolve_candidates([rating], index))

    assert len(results) == 1
    assert results[0] == {
        "org_name": "LCV",
        "year": 2024,
        "fec_candidate_id": "H8CA05036",
        "score": 100.0,
        "candidate_name": "Nancy Pelosi",
    }


def test_resolve_candidates_no_match_warns_and_skips(caplog):
    """Zero matches logs WARNING and skips the rating."""
    index = {}
    rating = _make_rating("Unknown Person", "XX")
    with caplog.at_level(logging.WARNING, logger="pipeline.processors.scorecard_resolver"):
        results = list(resolve_candidates([rating], index))

    assert results == []
    assert any("Unknown Person" in msg for msg in caplog.messages)


def test_resolve_candidates_ambiguous_warns_and_skips(caplog):
    """Two or more matches logs WARNING and skips the rating."""
    index = {("jane doe", "OH"): ["X1", "X2"]}
    rating = _make_rating("Jane Doe", "OH")
    with caplog.at_level(logging.WARNING, logger="pipeline.processors.scorecard_resolver"):
        results = list(resolve_candidates([rating], index))

    assert results == []
    assert any(
        "Jane Doe" in msg or "Ambiguous" in msg or "ambiguous" in msg for msg in caplog.messages
    )


def test_resolve_candidates_state_uppercased_defensively():
    """resolve_candidates normalizes state to uppercase regardless of fetcher."""
    index = {("ted cruz", "TX"): ["S0TX00453"]}
    # Simulate a fetcher that didn't uppercase (defensive test)
    rating = RawRating(
        org_name="LCV",
        year=2024,
        issue="environment",
        candidate_name="Ted Cruz",
        state="tx",  # lowercase state
        score=0.0,
    )
    results = list(resolve_candidates([rating], index))
    assert len(results) == 1
    assert results[0]["fec_candidate_id"] == "S0TX00453"


def test_resolve_candidates_multiple_ratings():
    """All ratings processed: some matched, some skipped."""
    index = {
        ("nancy pelosi", "CA"): ["H8CA05036"],
        ("ted cruz", "TX"): ["S0TX00453"],
    }
    ratings = [
        _make_rating("Nancy Pelosi", "CA", 100.0),
        _make_rating("Unknown Person", "XX", 50.0),  # no match → skip
        _make_rating("Ted Cruz", "TX", 0.0),
    ]
    results = list(resolve_candidates(ratings, index))
    assert len(results) == 2
    assert results[0]["fec_candidate_id"] == "H8CA05036"
    assert results[1]["fec_candidate_id"] == "S0TX00453"
