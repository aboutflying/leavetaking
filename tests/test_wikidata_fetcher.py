"""Tests for pipeline/fetchers/wikidata.py — find_corporation."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from pipeline.fetchers.wikidata import find_corporation

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_CORP_BINDING = {
    "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q185741"},
    "itemLabel": {"type": "literal", "value": "Procter & Gamble"},
    "parent": {"type": "uri", "value": "http://www.wikidata.org/entity/Q185741"},
    "parentLabel": {"type": "literal", "value": "Procter & Gamble"},
}

_SPARQL_RESPONSE_ONE = {
    "results": {"bindings": [_CORP_BINDING]}
}

_SPARQL_RESPONSE_EMPTY = {
    "results": {"bindings": []}
}


def _mock_get(json_data: dict):
    """Return a mock requests.Response that yields json_data."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = json_data
    return mock_resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFindCorporationUsesEntitySearch:
    def test_find_corporation_uses_entity_search_not_label_match(self):
        """Ensures we never revert to fragile exact-label SPARQL query.

        Bug caught: if find_corporation reverts to rdfs:label matching,
        brand variants like 'Procter & Gamble' silently return no results.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_SPARQL_RESPONSE_EMPTY)
            find_corporation("Apple")

            call_kwargs = mock_get.call_args
            sparql = call_kwargs[1]["params"]["query"]
            assert "EntitySearch" in sparql
            assert "rdfs:label" not in sparql

    def test_find_corporation_query_contains_corporation_filter(self):
        """Ensures P31 corporation filter is present in the SPARQL query.

        Bug caught: without the filter, EntitySearch returns people, places,
        and other non-corporate entities mixed into results.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_SPARQL_RESPONSE_EMPTY)
            find_corporation("Apple")

            sparql = mock_get.call_args[1]["params"]["query"]
            assert "Q783794" in sparql


class TestFindCorporationResults:
    def test_find_corporation_returns_results_for_name_variant(self):
        """Exact-label match returns [] for name variants; EntitySearch should not.

        Bug caught: exact rdfs:label match for 'Procter & Gamble' returns empty
        if Wikidata's preferred label differs slightly.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_SPARQL_RESPONSE_ONE)
            results = find_corporation("Procter & Gamble")

        assert len(results) == 1
        assert results[0]["qid"] == "Q185741"
        assert results[0]["name"] == "Procter & Gamble"

    def test_find_corporation_result_has_required_fields(self):
        """_extract_binding changes that silently drop qid or name are caught.

        Bug caught: refactoring _extract_binding could remove required fields;
        this ensures callers always get 'qid' and 'name'.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_SPARQL_RESPONSE_ONE)
            results = find_corporation("Procter & Gamble")

        assert len(results) == 1
        assert "qid" in results[0]
        assert "name" in results[0]
        # parent_qid and ticker are optional — just verify they don't raise
        _ = results[0].get("parent_qid")
        _ = results[0].get("ticker")


class TestFindCorporationEdgeCases:
    def test_find_corporation_returns_empty_list_for_empty_name(self):
        """Empty brand_name must not make an HTTP request or raise.

        Bug caught: a blank entry in TOP_BRANDS would send a useless query
        to Wikidata, wasting a rate-limit slot.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            result = find_corporation("")
            mock_get.assert_not_called()
        assert result == []

    def test_find_corporation_handles_double_quote_in_brand_name(self):
        """Brand name with double-quote must not raise or produce malformed SPARQL.

        Bug caught: brand_name='Intel "Core"' breaks SPARQL string formatting
        if the double-quote is not escaped before interpolation.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_SPARQL_RESPONSE_EMPTY)
            result = find_corporation('Intel "Core"')
        assert isinstance(result, list)

    def test_find_corporation_returns_empty_list_on_http_error(self):
        """HTTP 429 / 503 from Wikidata must not crash the pipeline.

        Bug caught: an unhandled HTTPError aborts batch_resolve_brands for
        all remaining brands, not just the failing one.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                raise_for_status=MagicMock(side_effect=requests.HTTPError("429"))
            )
            result = find_corporation("Apple")
        assert result == []
