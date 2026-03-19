"""Tests for API endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def mock_driver(client):
    """Patch app.state.neo4j_driver with a MagicMock for the duration of a test."""
    mock = MagicMock()
    mock.session.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock.session.return_value.__exit__ = MagicMock(return_value=False)
    app.state.neo4j_driver = mock
    return mock


class TestHealthEndpoint:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


class TestScoreEndpoints:
    def test_get_brand_scores(self, client, mock_driver):
        """Catches wrong response shape or missing brand key."""
        brand_scores = {
            "environment": {
                "League of Conservation Voters": {
                    "score": 72.5,
                    "dollars": 50000,
                    "candidates": 5,
                    "confidence": "high",
                    "cycles": [2022, 2024],
                    "computed_at": None,
                }
            }
        }
        with patch("api.routes.scores.query_brand_scores_from_graph", return_value=brand_scores):
            resp = client.get("/api/v1/scores/TestBrand")
        assert resp.status_code == 200
        data = resp.json()
        assert data["brand"] == "TestBrand"
        assert data["environment"]["League of Conservation Voters"]["score"] == 72.5

    def test_brand_not_found(self, client, mock_driver):
        """Catches wrong status code when BrandScore nodes are absent."""
        with patch("api.routes.scores.query_brand_scores_from_graph", return_value={}):
            resp = client.get("/api/v1/scores/NonExistent")
        assert resp.status_code == 404

    def test_search_scores(self, client, mock_driver):
        """Catches wrong count or missing brand in search results."""
        search_results = [
            {
                "brand": "TestBrand",
                "environment": {
                    "League of Conservation Voters": {
                        "score": 72.5,
                        "dollars": 50000,
                        "candidates": 5,
                        "confidence": "high",
                        "cycles": [2022, 2024],
                    }
                },
            }
        ]
        with patch("api.routes.scores.search_brand_scores_from_graph", return_value=search_results):
            resp = client.get("/api/v1/scores", params={"q": "Test"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["results"][0]["brand"] == "TestBrand"

    def test_search_with_issue_filter(self, client, mock_driver):
        """Catches issue filter not being forwarded to the query function."""
        with patch("api.routes.scores.search_brand_scores_from_graph", return_value=[]) as mock_fn:
            client.get("/api/v1/scores", params={"q": "Test", "issues": ["environment"]})
        _, kwargs = mock_fn.call_args
        assert kwargs["issues"] == ["environment"]


class TestConfigEndpoints:
    def test_get_default_preferences(self, client):
        resp = client.get("/api/v1/config/test-user-123")
        assert resp.status_code == 200
        data = resp.json()
        assert "issues" in data
        assert "trusted_scorecards" in data

    def test_update_preferences(self, client):
        prefs = {
            "issues": ["environment", "labor"],
            "trusted_scorecards": ["ACLU"],
            "issue_weights": {},
            "show_low_confidence": True,
            "badge_style": "detailed",
        }
        resp = client.put("/api/v1/config/test-user-456", json=prefs)
        assert resp.status_code == 200
        assert resp.json()["status"] == "updated"

        # Verify it was saved
        resp = client.get("/api/v1/config/test-user-456")
        assert resp.json()["issues"] == ["environment", "labor"]
        assert resp.json()["show_low_confidence"] is True

    def test_available_issues(self, client):
        resp = client.get("/api/v1/config/issues/available")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["issues"]) == 10
        assert len(data["scorecards"]) == 5
