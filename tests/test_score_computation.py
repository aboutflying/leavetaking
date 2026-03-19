"""Tests for score computation: dollar-weighted aggregation, confidence tiers, schema."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipeline.processors.score_computation import (
    _confidence,
    _weighted_score,
    compute_brand_scores,
)


class TestWeightedScore:
    def test_dollar_weighted_average(self):
        """Catches wrong formula — simple average would give 50.0, not 35.0."""
        rows = [{"score": 80.0, "dollars": 100.0}, {"score": 20.0, "dollars": 300.0}]
        result = _weighted_score(rows)
        assert result == pytest.approx(35.0, rel=1e-6)

    def test_zero_dollar_fallback_returns_unweighted_average(self):
        """Catches ZeroDivisionError or silent NaN when all dollars=0."""
        rows = [{"score": 80.0, "dollars": 0.0}, {"score": 20.0, "dollars": 0.0}]
        result = _weighted_score(rows)
        assert result == pytest.approx(50.0, rel=1e-6)

    def test_none_score_rows_are_skipped(self):
        """Catches TypeError from None * dollars."""
        rows = [{"score": None, "dollars": 100.0}, {"score": 60.0, "dollars": 200.0}]
        result = _weighted_score(rows)
        assert result == pytest.approx(60.0, rel=1e-6)

    def test_all_none_scores_returns_none(self):
        """Catches silent 0.0 return when no valid scores exist."""
        rows = [{"score": None, "dollars": 100.0}, {"score": None, "dollars": 200.0}]
        result = _weighted_score(rows)
        assert result is None

    def test_mixed_zero_dollars_and_valid_dollars(self):
        """When some rows have $0 and some have dollars, only non-zero rows contribute weight."""
        rows = [
            {"score": 80.0, "dollars": 0.0},
            {"score": 40.0, "dollars": 200.0},
        ]
        result = _weighted_score(rows)
        # Only the $200 row has weight — result should be 40.0, not (80+40)/2=60
        assert result == pytest.approx(40.0, rel=1e-6)


class TestConfidenceTiers:
    """Catches off-by-one on thresholds, wrong AND/OR logic."""

    def test_high_confidence(self):
        assert _confidence(150_000.0, 6) == "high"

    def test_medium_when_candidates_below_5_despite_high_dollars(self):
        assert _confidence(150_000.0, 4) == "medium"

    def test_medium_when_dollars_below_100k_despite_enough_candidates(self):
        assert _confidence(50_000.0, 5) == "medium"

    def test_medium_when_dollars_above_10k(self):
        assert _confidence(15_000.0, 1) == "medium"

    def test_medium_when_candidates_gte_2(self):
        assert _confidence(5_000.0, 3) == "medium"

    def test_low_confidence(self):
        assert _confidence(5_000.0, 1) == "low"

    def test_boundary_exactly_100k_is_not_high(self):
        """100_000 is NOT > 100_000 — boundary stays medium."""
        assert _confidence(100_000.0, 5) == "medium"

    def test_boundary_exactly_10k_is_not_medium_by_dollars(self):
        """10_000 is NOT > 10_000 — needs candidates >= 2 for medium."""
        assert _confidence(10_000.0, 1) == "low"


class TestOutputSchema:
    def test_output_schema_and_dollar_weighted_values(self):
        """Catches wrong grouping key, wrong dollar aggregation, wrong schema shape."""
        mock_session = MagicMock()
        mock_session.run.return_value = [
            {
                "issue": "environment",
                "scorecard": "LCV",
                "candidate_id": "C001",
                "score": 80.0,
                "dollars": 50_000.0,
            },
            {
                "issue": "environment",
                "scorecard": "LCV",
                "candidate_id": "C002",
                "score": 40.0,
                "dollars": 150_000.0,
            },
            {
                "issue": "labor",
                "scorecard": "AFL",
                "candidate_id": "C003",
                "score": 60.0,
                "dollars": 75_000.0,
            },
        ]

        result = compute_brand_scores(mock_session, "Nike", cycles=[2022, 2024])

        # Dollar-weighted: (80*50000 + 40*150000) / 200000 = 50.0
        assert result["environment"]["LCV"]["score"] == pytest.approx(50.0, rel=1e-3)
        assert result["environment"]["LCV"]["dollars"] == 200_000.0
        assert result["environment"]["LCV"]["candidates"] == 2
        assert result["labor"]["AFL"]["score"] == pytest.approx(60.0, rel=1e-3)
        assert result["labor"]["AFL"]["dollars"] == 75_000.0

    def test_candidate_dedup_across_paths(self):
        """Catches double-counting when same candidate appears from both PAC and exec paths."""
        mock_session = MagicMock()
        mock_session.run.return_value = [
            {
                "issue": "environment",
                "scorecard": "LCV",
                "candidate_id": "C001",
                "score": 80.0,
                "dollars": 50_000.0,
            },
            {
                "issue": "environment",
                "scorecard": "LCV",
                "candidate_id": "C001",
                "score": 80.0,
                "dollars": 30_000.0,
            },
        ]

        result = compute_brand_scores(mock_session, "Nike", cycles=[2022])

        assert result["environment"]["LCV"]["candidates"] == 1  # not 2
        assert result["environment"]["LCV"]["dollars"] == 80_000.0  # sum is correct

    def test_brand_no_data_returns_empty(self):
        """Catches crash or None return when brand has no ownership edges."""
        mock_session = MagicMock()
        mock_session.run.return_value = []

        result = compute_brand_scores(mock_session, "UnknownBrand", cycles=[2022])

        assert result == {}

    def test_multiple_scorecards_stay_separate(self):
        """Catches scorecard scores being merged — per-org breakdown must be preserved."""
        mock_session = MagicMock()
        mock_session.run.return_value = [
            {
                "issue": "environment",
                "scorecard": "LCV",
                "candidate_id": "C001",
                "score": 80.0,
                "dollars": 100_000.0,
            },
            {
                "issue": "environment",
                "scorecard": "ACLU",
                "candidate_id": "C002",
                "score": 20.0,
                "dollars": 100_000.0,
            },
        ]

        result = compute_brand_scores(mock_session, "Nike", cycles=[2022])

        assert result["environment"]["LCV"]["score"] == pytest.approx(80.0)
        assert result["environment"]["ACLU"]["score"] == pytest.approx(20.0)
        assert "LCV" in result["environment"]
        assert "ACLU" in result["environment"]

    def test_cycles_passed_as_query_parameter(self):
        """Catches cycle filter being hardcoded or omitted from query params."""
        mock_session = MagicMock()
        mock_session.run.return_value = []

        compute_brand_scores(mock_session, "Nike", cycles=[2020, 2022])

        call_kwargs = mock_session.run.call_args
        # cycles must be in the query parameters (positional or keyword)
        args, kwargs = call_kwargs
        all_params = {**kwargs}
        if len(args) > 1 and isinstance(args[1], dict):
            all_params.update(args[1])
        assert all_params.get("cycles") == [2020, 2022]
