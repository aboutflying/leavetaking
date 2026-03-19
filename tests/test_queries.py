"""Tests for API endpoints (no Neo4j required for score lookup tests)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def mock_scores(tmp_path):
    """Create a temporary scores file in v0.2 format."""
    scores = {
        "meta": {"version": "0.2", "brand_count": 2},
        "brands": {
            "TestBrand": {
                "environment": {
                    "League of Conservation Voters": {
                        "score": 72.5, "dollars": 50000,
                        "candidates": 5, "confidence": "high",
                    }
                },
                "labor": {
                    "AFL-CIO": {
                        "score": 45.0, "dollars": 25000,
                        "candidates": 3, "confidence": "medium",
                    }
                },
            },
            "AnotherBrand": {
                "environment": {
                    "League of Conservation Voters": {
                        "score": 30.0, "dollars": 10000,
                        "candidates": 2, "confidence": "low",
                    }
                },
            },
        },
    }
    path = tmp_path / "scores.json"
    path.write_text(json.dumps(scores))
    return path


class TestHealthEndpoint:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


class TestScoreEndpoints:
    def test_get_brand_scores(self, client, mock_scores):
        from api.routes import scores as scores_module

        scores_module._scores_cache = None
        with patch.object(scores_module.settings, "scores_output", mock_scores):
            resp = client.get("/api/v1/scores/TestBrand")
            assert resp.status_code == 200
            data = resp.json()
            assert data["brand"] == "TestBrand"
            assert "environment" in data
            assert data["environment"]["League of Conservation Voters"]["score"] == 72.5

    def test_brand_not_found(self, client, mock_scores):
        from api.routes import scores as scores_module

        scores_module._scores_cache = None
        with patch.object(scores_module.settings, "scores_output", mock_scores):
            resp = client.get("/api/v1/scores/NonExistent")
            assert resp.status_code == 404

    def test_search_scores(self, client, mock_scores):
        from api.routes import scores as scores_module

        scores_module._scores_cache = None
        with patch.object(scores_module.settings, "scores_output", mock_scores):
            resp = client.get("/api/v1/scores", params={"q": "Test"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 1
            assert data["results"][0]["brand"] == "TestBrand"

    def test_search_with_issue_filter(self, client, mock_scores):
        from api.routes import scores as scores_module

        scores_module._scores_cache = None
        with patch.object(scores_module.settings, "scores_output", mock_scores):
            resp = client.get(
                "/api/v1/scores",
                params={"q": "Test", "issues": ["environment"]},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert "environment" in data["results"][0]


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
