"""Tests for score computation: dollar-weighted aggregation, confidence tiers, schema."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipeline.processors.score_computation import (
    _confidence,
    _weighted_score,
    compute_brand_scores,
    query_brand_scores_from_graph,
    write_brand_scores,
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


class TestWriteBrandScores:
    def test_writes_one_row_per_brand_issue_scorecard(self):
        """Catches wrong row shape or missing brands in the UNWIND payload."""
        mock_session = MagicMock()
        scores = {
            "Nike": {
                "environment": {
                    "LCV": {"score": 72.0, "dollars": 200_000, "candidates": 6, "confidence": "high"},
                },
                "labor": {
                    "AFL-CIO": {"score": 55.0, "dollars": 50_000, "candidates": 2, "confidence": "medium"},
                },
            },
            "Adidas": {
                "environment": {
                    "LCV": {"score": 40.0, "dollars": 0, "candidates": 1, "confidence": "low"},
                },
            },
        }

        count = write_brand_scores(mock_session, scores, cycles=[2022, 2024])

        assert count == 3  # 2 Nike rows + 1 Adidas row
        mock_session.run.assert_called_once()
        _, kwargs = mock_session.run.call_args
        rows = kwargs["rows"]
        assert len(rows) == 3

    def test_row_shape_includes_all_required_fields(self):
        """Catches missing fields that the Cypher query expects."""
        mock_session = MagicMock()
        scores = {
            "Nike": {
                "environment": {
                    "LCV": {"score": 72.0, "dollars": 100_000, "candidates": 5, "confidence": "high"},
                }
            }
        }

        write_brand_scores(mock_session, scores, cycles=[2024])

        _, kwargs = mock_session.run.call_args
        row = kwargs["rows"][0]
        assert row["brand_name"] == "Nike"
        assert row["issue_name"] == "environment"
        assert row["scorecard_org"] == "LCV"
        assert row["score"] == 72.0
        assert row["dollars"] == 100_000
        assert row["candidates"] == 5
        assert row["confidence"] == "high"
        assert row["cycles"] == [2024]

    def test_empty_scores_skips_db_call(self):
        """Catches unnecessary DB write when there is nothing to persist."""
        mock_session = MagicMock()

        count = write_brand_scores(mock_session, {}, cycles=[2024])

        assert count == 0
        mock_session.run.assert_not_called()

    def test_cycles_stored_on_every_row(self):
        """Catches cycles being attached to only the first row or omitted."""
        mock_session = MagicMock()
        scores = {
            "Nike": {
                "environment": {"LCV": {"score": 60.0, "dollars": 10_000, "candidates": 2, "confidence": "medium"}},
                "labor": {"AFL-CIO": {"score": 50.0, "dollars": 5_000, "candidates": 1, "confidence": "low"}},
            }
        }

        write_brand_scores(mock_session, scores, cycles=[2022, 2024])

        _, kwargs = mock_session.run.call_args
        for row in kwargs["rows"]:
            assert row["cycles"] == [2022, 2024]


class TestQueryBrandScoresFromGraph:
    def test_reconstructs_nested_dict_from_graph_records(self):
        """Catches wrong nesting or missing fields in the reconstructed payload."""
        mock_session = MagicMock()
        mock_session.run.return_value = [
            {
                "issue": "environment",
                "scorecard": "LCV",
                "score": 72.0,
                "dollars": 200_000,
                "candidates": 6,
                "confidence": "high",
                "cycles": [2022, 2024],
                "computed_at": "2026-03-19T00:00:00Z",
            },
            {
                "issue": "labor",
                "scorecard": "AFL-CIO",
                "score": 55.0,
                "dollars": 50_000,
                "candidates": 2,
                "confidence": "medium",
                "cycles": [2022, 2024],
                "computed_at": "2026-03-19T00:00:00Z",
            },
        ]

        result = query_brand_scores_from_graph(mock_session, "Nike")

        assert result["environment"]["LCV"]["score"] == 72.0
        assert result["environment"]["LCV"]["confidence"] == "high"
        assert result["labor"]["AFL-CIO"]["dollars"] == 50_000

    def test_returns_empty_dict_when_no_brand_score_nodes(self):
        """Catches crash or None return when brand has no BrandScore nodes."""
        mock_session = MagicMock()
        mock_session.run.return_value = []

        result = query_brand_scores_from_graph(mock_session, "UnknownBrand")

        assert result == {}

    def test_issue_and_scorecard_filters_passed_to_query(self):
        """Catches filters being ignored or hardcoded."""
        mock_session = MagicMock()
        mock_session.run.return_value = []

        query_brand_scores_from_graph(
            mock_session,
            "Nike",
            issues=["environment"],
            scorecards=["LCV"],
        )

        _, kwargs = mock_session.run.call_args
        assert kwargs["issues"] == ["environment"]
        assert kwargs["scorecards"] == ["LCV"]
        assert kwargs["brand_name"] == "Nike"
