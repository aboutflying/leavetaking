"""Tests for pipeline/fetchers/scorecards.py — RawRating, LCVFetcher, registry."""

from __future__ import annotations

import csv
import logging
from pathlib import Path

import pytest

from pipeline.fetchers.scorecards import (
    FETCHER_REGISTRY,
    LCVFetcher,
    load_all_scorecards,
    normalize_score,
)


# ---------------------------------------------------------------------------
# normalize_score — grade conversion and numeric passthrough
# ---------------------------------------------------------------------------

def test_grade_conversion_a_plus():
    assert normalize_score("A+") == 100.0


def test_grade_conversion_f():
    assert normalize_score("F") == 0.0


def test_grade_conversion_b_minus():
    assert normalize_score("B-") == 70.0


def test_grade_conversion_numeric_float():
    assert normalize_score(85) == 85.0


def test_grade_conversion_numeric_string():
    assert normalize_score("73.5") == 73.5


def test_grade_conversion_unknown_raises():
    """Unknown grade strings must raise ValueError, not silently corrupt data."""
    with pytest.raises(ValueError, match="X"):
        normalize_score("X")


def test_grade_conversion_empty_string_raises():
    """Blank score cell must not silently pass through."""
    with pytest.raises(ValueError):
        normalize_score("")


# ---------------------------------------------------------------------------
# LCVFetcher — CSV parsing and edge cases
# ---------------------------------------------------------------------------

def _write_lcv_csv(tmp_path: Path, year: int, rows: list[dict]) -> Path:
    """Write a minimal LCV-style CSV and return the path."""
    path = tmp_path / f"lcv_{year}.csv"
    score_col = f"{year} Score"
    fieldnames = ["Member", "State", "Party", score_col, "Lifetime Score"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def test_lcv_fetcher_yields_raw_ratings(tmp_path):
    """LCVFetcher parses CSV and yields correct RawRating fields."""
    _write_lcv_csv(tmp_path, 2024, [
        {"Member": "Nancy Pelosi", "State": "CA", "Party": "D", "2024 Score": "100", "Lifetime Score": "97"},
    ])
    fetcher = LCVFetcher(tmp_path)
    ratings = list(fetcher.fetch(2024))

    assert len(ratings) == 1
    r = ratings[0]
    assert r.org_name == "League of Conservation Voters"
    assert r.candidate_name == "Nancy Pelosi"
    assert r.state == "CA"
    assert r.score == 100.0
    assert r.issue == "environment"
    assert r.year == 2024


def test_lcv_fetcher_state_uppercased(tmp_path):
    """State field is uppercased regardless of CSV casing."""
    _write_lcv_csv(tmp_path, 2024, [
        {"Member": "Ted Cruz", "State": "tx", "Party": "R", "2024 Score": "0", "Lifetime Score": "3"},
    ])
    fetcher = LCVFetcher(tmp_path)
    ratings = list(fetcher.fetch(2024))

    assert ratings[0].state == "TX"


def test_lcv_fetcher_skips_missing_file(tmp_path):
    """Missing file yields nothing and does not raise."""
    fetcher = LCVFetcher(tmp_path)  # no CSV written
    ratings = list(fetcher.fetch(2024))
    assert ratings == []


def test_lcv_fetcher_score_column_detection(tmp_path):
    """Year-specific score column (e.g. '2024 Score') is detected and parsed."""
    _write_lcv_csv(tmp_path, 2024, [
        {"Member": "AOC", "State": "NY", "Party": "D", "2024 Score": "95", "Lifetime Score": "95"},
    ])
    fetcher = LCVFetcher(tmp_path)
    ratings = list(fetcher.fetch(2024))

    assert len(ratings) == 1
    assert ratings[0].score == 95.0


def test_lcv_fetcher_score_column_not_found_logs_warning(tmp_path, caplog):
    """CSV with wrong year's column logs WARNING and yields nothing."""
    # Write a CSV with '2022 Score' but fetch for year 2024
    path = tmp_path / "lcv_2024.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["Member", "State", "Party", "2022 Score", "Lifetime Score"])
        writer.writeheader()
        writer.writerow({"Member": "Jane Doe", "State": "OH", "Party": "D", "2022 Score": "80", "Lifetime Score": "70"})

    fetcher = LCVFetcher(tmp_path)
    with caplog.at_level(logging.WARNING, logger="pipeline.fetchers.scorecards"):
        ratings = list(fetcher.fetch(2024))

    assert ratings == []
    assert any("2024" in msg for msg in caplog.messages)


def test_lcv_fetcher_blank_score_row_skipped(tmp_path, caplog):
    """Rows with blank score are skipped with a warning; valid rows still yielded."""
    path = tmp_path / "lcv_2024.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["Member", "State", "Party", "2024 Score", "Lifetime Score"])
        writer.writeheader()
        writer.writerow({"Member": "Jane Doe", "State": "OH", "Party": "D", "2024 Score": "", "Lifetime Score": "70"})
        writer.writerow({"Member": "John Smith", "State": "TX", "Party": "R", "2024 Score": "10", "Lifetime Score": "15"})

    fetcher = LCVFetcher(tmp_path)
    with caplog.at_level(logging.WARNING, logger="pipeline.fetchers.scorecards"):
        ratings = list(fetcher.fetch(2024))

    assert len(ratings) == 1
    assert ratings[0].candidate_name == "John Smith"


# ---------------------------------------------------------------------------
# FETCHER_REGISTRY and load_all_scorecards
# ---------------------------------------------------------------------------

def test_fetcher_registry_contains_lcv():
    """FETCHER_REGISTRY maps 'League of Conservation Voters' to an LCVFetcher."""
    assert "League of Conservation Voters" in FETCHER_REGISTRY
    assert isinstance(FETCHER_REGISTRY["League of Conservation Voters"], LCVFetcher)


def test_load_all_scorecards_iterates_registry(tmp_path, monkeypatch):
    """load_all_scorecards yields RawRatings from each registered fetcher."""
    _write_lcv_csv(tmp_path, 2024, [
        {"Member": "Nancy Pelosi", "State": "CA", "Party": "D", "2024 Score": "100", "Lifetime Score": "97"},
    ])
    mock_registry = {"League of Conservation Voters": LCVFetcher(tmp_path)}
    monkeypatch.setattr("pipeline.fetchers.scorecards.FETCHER_REGISTRY", mock_registry)

    ratings = list(load_all_scorecards([2024]))
    assert len(ratings) == 1
    assert ratings[0].org_name == "League of Conservation Voters"


# ---------------------------------------------------------------------------
# JsonFileFetcher — JSON parsing and edge cases
# ---------------------------------------------------------------------------

from pipeline.fetchers.scorecards import JsonFileFetcher  # noqa: E402


def _write_scorecard_json(tmp_path: Path, org_name: str, year: int, issue: str, ratings: list[dict]) -> Path:
    """Write a minimal scorecard JSON file and return the path."""
    import json
    slug = org_name.lower().replace(" ", "_")
    path = tmp_path / f"{slug}_{year}.json"
    path.write_text(json.dumps({"org_name": org_name, "year": year, "issue": issue, "ratings": ratings}), encoding="utf-8")
    return path


def test_json_file_fetcher_yields_raw_ratings(tmp_path):
    """JsonFileFetcher parses JSON and yields correct RawRating fields."""
    _write_scorecard_json(tmp_path, "ACLU", 2024, "civil_liberties", [
        {"candidate_name": "Nancy Pelosi", "state": "CA", "score": 95},
    ])
    fetcher = JsonFileFetcher(tmp_path, "ACLU", "civil_liberties")
    ratings = list(fetcher.fetch(2024))

    assert len(ratings) == 1
    r = ratings[0]
    assert r.org_name == "ACLU"
    assert r.candidate_name == "Nancy Pelosi"
    assert r.state == "CA"
    assert r.score == 95.0
    assert r.year == 2024
    assert r.issue == "civil_liberties"


def test_json_file_fetcher_letter_grade_converted(tmp_path):
    """Letter grade scores are converted to 0-100 float via normalize_score."""
    _write_scorecard_json(tmp_path, "ACLU", 2024, "civil_liberties", [
        {"candidate_name": "Ted Cruz", "state": "TX", "score": "B+"},
    ])
    fetcher = JsonFileFetcher(tmp_path, "ACLU", "civil_liberties")
    ratings = list(fetcher.fetch(2024))

    assert len(ratings) == 1
    assert ratings[0].score == 85.0


def test_json_file_fetcher_missing_file_yields_nothing(tmp_path):
    """Missing JSON file yields nothing and does not raise."""
    fetcher = JsonFileFetcher(tmp_path, "ACLU", "civil_liberties")
    ratings = list(fetcher.fetch(2024))
    assert ratings == []


def test_json_file_fetcher_skips_row_missing_state(tmp_path, caplog):
    """Ratings with missing state are skipped with a WARNING."""
    _write_scorecard_json(tmp_path, "ACLU", 2024, "civil_liberties", [
        {"candidate_name": "Nancy Pelosi", "score": 95},  # no state
        {"candidate_name": "Ted Cruz", "state": "TX", "score": 10},
    ])
    fetcher = JsonFileFetcher(tmp_path, "ACLU", "civil_liberties")
    with caplog.at_level(logging.WARNING, logger="pipeline.fetchers.scorecards"):
        ratings = list(fetcher.fetch(2024))

    assert len(ratings) == 1
    assert ratings[0].candidate_name == "Ted Cruz"
    assert any("state" in msg.lower() or "missing" in msg.lower() for msg in caplog.messages)


def test_json_file_fetcher_skips_row_missing_candidate_name(tmp_path, caplog):
    """Ratings with missing candidate_name are skipped with a WARNING."""
    _write_scorecard_json(tmp_path, "ACLU", 2024, "civil_liberties", [
        {"state": "CA", "score": 95},  # no candidate_name
        {"candidate_name": "Ted Cruz", "state": "TX", "score": 10},
    ])
    fetcher = JsonFileFetcher(tmp_path, "ACLU", "civil_liberties")
    with caplog.at_level(logging.WARNING, logger="pipeline.fetchers.scorecards"):
        ratings = list(fetcher.fetch(2024))

    assert len(ratings) == 1
    assert ratings[0].candidate_name == "Ted Cruz"


def test_json_file_fetcher_malformed_json_logs_error_and_yields_nothing(tmp_path, caplog):
    """Malformed JSON file logs ERROR and yields nothing — pipeline must not crash."""
    slug = "aclu"
    path = tmp_path / f"{slug}_2024.json"
    path.write_text("not valid json {{{{", encoding="utf-8")

    fetcher = JsonFileFetcher(tmp_path, "ACLU", "civil_liberties")
    with caplog.at_level(logging.ERROR, logger="pipeline.fetchers.scorecards"):
        ratings = list(fetcher.fetch(2024))

    assert ratings == []
    assert any("malformed" in msg.lower() or "json" in msg.lower() for msg in caplog.messages)


def test_json_file_fetcher_skips_blank_score(tmp_path, caplog):
    """Blank/NA score values are silently skipped at DEBUG level (not WARNING)."""
    _write_scorecard_json(tmp_path, "ACLU", 2024, "civil_liberties", [
        {"candidate_name": "Jane Doe", "state": "OH", "score": ""},
        {"candidate_name": "John Smith", "state": "TX", "score": 50},
    ])
    fetcher = JsonFileFetcher(tmp_path, "ACLU", "civil_liberties")
    with caplog.at_level(logging.WARNING, logger="pipeline.fetchers.scorecards"):
        ratings = list(fetcher.fetch(2024))

    assert len(ratings) == 1
    assert ratings[0].candidate_name == "John Smith"
    # Blank score should NOT produce a WARNING (it's a known skip value, not an error)
    warning_msgs = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert not any("Jane Doe" in m for m in warning_msgs)
