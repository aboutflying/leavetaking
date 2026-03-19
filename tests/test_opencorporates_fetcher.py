"""Tests for pipeline/fetchers/opencorporates.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests

import pipeline.fetchers.opencorporates as oc_module
from pipeline.fetchers.opencorporates import (
    get_company,
    get_corporate_grouping,
    get_subsidiary_statements,
    search_companies,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_COMPANIES_RESPONSE = {
    "results": {
        "companies": [
            {"company": {"name": "Bose Corporation", "company_number": "123", "jurisdiction_code": "us_ma"}},
        ]
    }
}

_COMPANY_RESPONSE = {
    "results": {
        "company": {"name": "Bose Corporation", "company_number": "123", "jurisdiction_code": "us_ma"}
    }
}

_GROUPING_RESPONSE = {
    "results": {
        "corporate_groupings": [
            {"corporate_grouping": {"name": "Bose Group", "wikipedia_id": "Bose_Corporation"}}
        ]
    }
}

_STATEMENTS_RESPONSE = {
    "results": {
        "statements": [
            {"statement": {"predicate": "subsidiary_of", "object": {"name": "Parent Corp"}}},
            {"statement": {"predicate": "other_type", "object": {"name": "Other"}}},
        ]
    }
}


def _mock_ok(json_data: dict):
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.status_code = 200
    mock.json.return_value = json_data
    return mock


def _mock_401():
    mock = MagicMock()
    mock.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")
    mock.status_code = 401
    return mock


# ---------------------------------------------------------------------------
# Tests: no-token early exit
# ---------------------------------------------------------------------------


class TestNoTokenEarlyExit:
    """When no API token is configured, no HTTP request should be made."""

    def test_search_companies_skips_request_when_no_token(self):
        """search_companies must return [] without calling requests.get when token is unset.

        Root cause: OpenCorporates returns 401 when no api_token is provided.
        The fix skips the HTTP call entirely when the token is not configured,
        preventing predictable 401 errors from polluting the logs on every run.
        """
        with patch.object(oc_module.settings, "opencorporates_api_token", ""):
            with patch("pipeline.fetchers.opencorporates.requests.get") as mock_get:
                result = search_companies("Bose")

        assert result == []
        mock_get.assert_not_called()

    def test_get_company_returns_none_when_no_token(self):
        """get_company must return None without HTTP call when token is unset."""
        with patch.object(oc_module.settings, "opencorporates_api_token", ""):
            with patch("pipeline.fetchers.opencorporates.requests.get") as mock_get:
                result = get_company("us_ma", "123")

        assert result is None
        mock_get.assert_not_called()

    def test_get_corporate_grouping_returns_none_when_no_token(self):
        """get_corporate_grouping must return None without HTTP call when token is unset."""
        with patch.object(oc_module.settings, "opencorporates_api_token", ""):
            with patch("pipeline.fetchers.opencorporates.requests.get") as mock_get:
                result = get_corporate_grouping("Bose")

        assert result is None
        mock_get.assert_not_called()

    def test_get_subsidiary_statements_returns_empty_when_no_token(self):
        """get_subsidiary_statements must return [] without HTTP call when token is unset."""
        with patch.object(oc_module.settings, "opencorporates_api_token", ""):
            with patch("pipeline.fetchers.opencorporates.requests.get") as mock_get:
                result = get_subsidiary_statements("us_ma", "123")

        assert result == []
        mock_get.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: normal operation with token
# ---------------------------------------------------------------------------


class TestWithToken:
    def test_search_companies_returns_results(self):
        with patch.object(oc_module.settings, "opencorporates_api_token", "test-token"):
            with patch("pipeline.fetchers.opencorporates.requests.get") as mock_get:
                mock_get.return_value = _mock_ok(_COMPANIES_RESPONSE)
                results = search_companies("Bose")

        assert len(results) == 1
        assert results[0]["name"] == "Bose Corporation"

    def test_search_companies_includes_token_in_params(self):
        with patch.object(oc_module.settings, "opencorporates_api_token", "test-token"):
            with patch("pipeline.fetchers.opencorporates.requests.get") as mock_get:
                mock_get.return_value = _mock_ok({"results": {"companies": []}})
                search_companies("Bose")

        params = mock_get.call_args[1]["params"]
        assert params.get("api_token") == "test-token"

    def test_get_company_returns_data(self):
        with patch.object(oc_module.settings, "opencorporates_api_token", "test-token"):
            with patch("pipeline.fetchers.opencorporates.requests.get") as mock_get:
                mock_get.return_value = _mock_ok(_COMPANY_RESPONSE)
                result = get_company("us_ma", "123")

        assert result["name"] == "Bose Corporation"

    def test_get_company_returns_none_on_404(self):
        with patch.object(oc_module.settings, "opencorporates_api_token", "test-token"):
            with patch("pipeline.fetchers.opencorporates.requests.get") as mock_get:
                mock_get.return_value = MagicMock(status_code=404)
                result = get_company("us_ma", "999")

        assert result is None

    def test_get_subsidiary_statements_filters_to_relationship_types(self):
        with patch.object(oc_module.settings, "opencorporates_api_token", "test-token"):
            with patch("pipeline.fetchers.opencorporates.requests.get") as mock_get:
                mock_get.return_value = _mock_ok(_STATEMENTS_RESPONSE)
                results = get_subsidiary_statements("us_ma", "123")

        assert len(results) == 1
        assert results[0]["predicate"] == "subsidiary_of"
