"""Tests for pipeline/fetchers/wikidata.py — find_corporation."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import requests

from pipeline.fetchers.wikidata import _search_entities, find_corporation, query_sparql

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_CORP_BINDING = {
    "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q185741"},
    "itemLabel": {"type": "literal", "value": "Procter & Gamble"},
    "parent": {"type": "uri", "value": "http://www.wikidata.org/entity/Q185741"},
    "parentLabel": {"type": "literal", "value": "Procter & Gamble"},
}

_SPARQL_RESPONSE_ONE = {"results": {"bindings": [_CORP_BINDING]}}

_SPARQL_RESPONSE_EMPTY = {"results": {"bindings": []}}

# wbsearchentities API response (step 1 of the two-step strategy)
_WBSEARCH_RESPONSE_ONE = {
    "search": [{"id": "Q185741", "label": "Procter & Gamble", "description": "American consumer goods company"}]
}

_WBSEARCH_RESPONSE_EMPTY = {"search": []}


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
    def test_find_corporation_does_not_use_mwapi_in_sparql(self):
        """Regression test: mwapi EntitySearch inside SPARQL causes HTTP 500 on Wikidata.

        Root cause: wikibase:mwapi EntitySearch SERVICE fails for popular brand names
        (e.g. 'Apple') due to Wikidata query engine timeouts → HTTP 500 returned.
        Fix: use wbsearchentities Action API for fuzzy search (step 1), then query
        SPARQL with VALUES + specific QIDs (step 2). No mwapi in SPARQL.
        """
        wbsearch_resp = _mock_get(_WBSEARCH_RESPONSE_EMPTY)
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = wbsearch_resp
            find_corporation("Apple")

            # With the two-step fix, the first call goes to wbsearchentities,
            # not to the SPARQL endpoint with mwapi. If wbsearchentities returns
            # no results, SPARQL is never called, so we just check call count = 1.
            assert mock_get.call_count >= 1
            first_call_url = mock_get.call_args_list[0][0][0]
            assert "wbsearchentities" in str(first_call_url) or "w/api.php" in str(first_call_url), (
                "First request should go to wbsearchentities API, not SPARQL with mwapi"
            )

    def test_find_corporation_sparql_does_not_contain_mwapi(self):
        """The SPARQL query must not use wikibase:mwapi (causes 500 for popular names).

        When QIDs are found via wbsearchentities, the SPARQL uses VALUES, not mwapi.
        """
        wbsearch_resp = _mock_get(_WBSEARCH_RESPONSE_ONE)
        sparql_resp = _mock_get(_SPARQL_RESPONSE_EMPTY)
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.side_effect = [wbsearch_resp, sparql_resp]
            find_corporation("Apple")

            assert mock_get.call_count == 2
            sparql_call = mock_get.call_args_list[1]
            sparql = sparql_call[1]["params"]["query"]
            assert "mwapi" not in sparql, "SPARQL must not use wikibase:mwapi (causes 500)"
            assert "EntitySearch" not in sparql, "SPARQL must not use EntitySearch (causes 500)"
            assert "rdfs:label" not in sparql

    def test_find_corporation_query_contains_corporation_filter(self):
        """Ensures P31 business-entity filter uses Q4830453 (business enterprise).

        Bug caught: Q783794 (corporation) is too narrow — public companies,
        private companies, LLCs etc. are subclasses of Q4830453 not Q783794.
        Using Q783794 causes Bose, Duracell, SC Johnson, L'Oréal to return
        zero rows from SPARQL even though wbsearchentities found their QIDs.
        """
        wbsearch_resp = _mock_get(_WBSEARCH_RESPONSE_ONE)
        sparql_resp = _mock_get(_SPARQL_RESPONSE_EMPTY)
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.side_effect = [wbsearch_resp, sparql_resp]
            find_corporation("Apple")

            sparql_call = mock_get.call_args_list[1]
            sparql = sparql_call[1]["params"]["query"]
            assert "Q4830453" in sparql, "SPARQL must use Q4830453 (business enterprise)"
            assert "Q783794" not in sparql, "Q783794 (corporation) is too narrow — must use Q4830453"


class TestFindCorporationResults:
    def test_find_corporation_returns_results_for_name_variant(self):
        """Fuzzy search returns results for name variants; exact label match would not.

        Bug caught: exact rdfs:label match for 'Procter & Gamble' returns empty
        if Wikidata's preferred label differs slightly. The two-step approach
        (wbsearchentities + SPARQL VALUES) handles variants correctly.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.side_effect = [_mock_get(_WBSEARCH_RESPONSE_ONE), _mock_get(_SPARQL_RESPONSE_ONE)]
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
            mock_get.side_effect = [_mock_get(_WBSEARCH_RESPONSE_ONE), _mock_get(_SPARQL_RESPONSE_ONE)]
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
        """Brand name with double-quote must not raise.

        The wbsearchentities API accepts the brand name as a plain string parameter,
        so no SPARQL escaping is needed for the search step. The SPARQL VALUES clause
        uses QIDs (e.g. wd:Q123), which are never derived from the raw brand name.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_WBSEARCH_RESPONSE_EMPTY)
            result = find_corporation('Intel "Core"')
        assert isinstance(result, list)

    def test_find_corporation_returns_empty_list_on_http_error(self):
        """HTTP error from wbsearchentities step must not crash the pipeline.

        Bug caught: an unhandled HTTPError aborts batch_resolve_brands for
        all remaining brands, not just the failing one.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                raise_for_status=MagicMock(side_effect=requests.HTTPError("429"))
            )
            result = find_corporation("Apple")
        assert result == []

    def test_find_corporation_returns_empty_list_when_no_qids_found(self):
        """When wbsearchentities returns no results, return [] without calling SPARQL."""
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_WBSEARCH_RESPONSE_EMPTY)
            result = find_corporation("NonexistentBrandXYZ")

        assert result == []
        assert mock_get.call_count == 1, "SPARQL should not be called when no QIDs found"


# ---------------------------------------------------------------------------
# Tests: retry on transient 5xx errors
# ---------------------------------------------------------------------------


class TestTransientRetry:
    def test_query_sparql_retries_on_502_and_succeeds(self):
        """query_sparql must retry on 502 Bad Gateway and return results on success.

        Root cause: Wikidata SPARQL endpoint returns 502 transiently.
        Without retries, find_corporation returns [] for the entire brand,
        discarding valid data due to a temporary infrastructure blip.
        Fix: retry up to 3 times with exponential backoff for 5xx errors.
        """
        ok_resp = _mock_get({"results": {"bindings": [_CORP_BINDING]}})
        err_resp = MagicMock(raise_for_status=MagicMock(side_effect=requests.HTTPError("502")))

        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            with patch("pipeline.fetchers.wikidata.time.sleep"):
                mock_get.side_effect = [err_resp, ok_resp]
                results = query_sparql("SELECT ?item WHERE { wd:Q312 ?p ?o } LIMIT 1")

        assert len(results) == 1
        assert mock_get.call_count == 2

    def test_query_sparql_returns_empty_after_all_retries_exhausted(self):
        """query_sparql raises after all retries fail on persistent 5xx."""
        err_resp = MagicMock(raise_for_status=MagicMock(side_effect=requests.HTTPError("502")))

        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            with patch("pipeline.fetchers.wikidata.time.sleep"):
                mock_get.return_value = err_resp
                try:
                    query_sparql("SELECT ?item WHERE { wd:Q312 ?p ?o } LIMIT 1")
                    raised = False
                except requests.HTTPError:
                    raised = True

        assert raised, "Should re-raise after retries exhausted"
        assert mock_get.call_count == 3, "Should attempt exactly 3 times"

    def test_query_sparql_does_not_retry_on_400(self):
        """query_sparql must not retry on 400 Bad Request — that's a permanent error."""
        err_resp = MagicMock(raise_for_status=MagicMock(side_effect=requests.HTTPError("400")))

        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            with patch("pipeline.fetchers.wikidata.time.sleep") as mock_sleep:
                mock_get.return_value = err_resp
                try:
                    query_sparql("SELECT ?item WHERE { INVALID SPARQL } LIMIT 1")
                except requests.HTTPError:
                    pass

        assert mock_get.call_count == 1, "Should not retry on 4xx"
        mock_sleep.assert_not_called()

    def test_find_corporation_succeeds_when_sparql_502_then_ok(self):
        """find_corporation returns results when SPARQL gets a transient 502 then succeeds."""
        wbsearch_resp = _mock_get(_WBSEARCH_RESPONSE_ONE)
        sparql_err = MagicMock(raise_for_status=MagicMock(side_effect=requests.HTTPError("502")))
        sparql_ok = _mock_get(_SPARQL_RESPONSE_ONE)

        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            with patch("pipeline.fetchers.wikidata.time.sleep"):
                mock_get.side_effect = [wbsearch_resp, sparql_err, sparql_ok]
                results = find_corporation("3M")

        assert len(results) == 1


# ---------------------------------------------------------------------------
# Tests: _search_entities alias capture
# ---------------------------------------------------------------------------

_WBSEARCH_ALIAS_MATCH = {
    "search": [
        {
            "id": "Q336958",
            "label": "Advanced Micro Devices",
            "description": "American semiconductor company",
            "match": {"type": "alias", "language": "en", "text": "AMD"},
        }
    ]
}

# SPARQL binding for Q336958 (AMD) — QID matches _WBSEARCH_ALIAS_MATCH
_SPARQL_RESPONSE_AMD = {
    "results": {
        "bindings": [
            {
                "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q336958"},
                "itemLabel": {"type": "literal", "value": "Advanced Micro Devices"},
            }
        ]
    }
}

_WBSEARCH_LABEL_MATCH = {
    "search": [
        {
            "id": "Q312",
            "label": "Apple Inc.",
            "description": "American technology company",
            "match": {"type": "label", "language": "en", "text": "Apple Inc."},
        }
    ]
}

_WBSEARCH_ALIAS_NO_TEXT = {
    "search": [
        {
            "id": "Q336958",
            "label": "Advanced Micro Devices",
            "match": {"type": "alias"},  # 'text' key absent
        }
    ]
}


class TestSearchEntitiesAliasCapture:
    def test_search_entities_returns_alias_when_match_is_alias(self):
        """When wbsearchentities matches on an alias, matched_alias is captured.

        Bug caught: alias text discarded — AMD scores ~0.2 against
        'Advanced Micro Devices' and never resolves.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_WBSEARCH_ALIAS_MATCH)
            results = _search_entities("AMD")

        assert len(results) == 1
        assert results[0]["qid"] == "Q336958"
        assert results[0]["matched_alias"] == "AMD"

    def test_search_entities_returns_none_alias_when_match_is_label(self):
        """When wbsearchentities matches on label (not alias), matched_alias is None.

        Bug caught: label match incorrectly sets alias field, polluting scoring.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_WBSEARCH_LABEL_MATCH)
            results = _search_entities("Apple")

        assert len(results) == 1
        assert results[0]["qid"] == "Q312"
        assert results[0]["matched_alias"] is None

    def test_search_entities_match_text_absent_returns_none_alias(self):
        """match.type=='alias' with no 'text' key must not raise KeyError.

        Bug caught: item['match']['text'] raises KeyError when text key absent.
        Fix: use .get('text') not ['text'].
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_WBSEARCH_ALIAS_NO_TEXT)
            results = _search_entities("AMD")

        assert len(results) == 1
        assert results[0]["matched_alias"] is None


class TestFindCorporationAliasThreading:
    def test_find_corporation_passes_alias_to_result(self):
        """Alias captured by _search_entities must appear in find_corporation result.

        Bug caught: alias not threaded through find_corporation → _extract_binding,
        so _get_scored_candidates never sees it and AMD scores ~0.2.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.side_effect = [
                _mock_get(_WBSEARCH_ALIAS_MATCH),   # Q336958 with alias "AMD"
                _mock_get(_SPARQL_RESPONSE_AMD),     # SPARQL returns Q336958 → alias lookup hits
            ]
            results = find_corporation("AMD")

        assert len(results) == 1
        assert results[0]["alias"] == "AMD"

    def test_find_corporation_no_alias_key_when_label_match(self):
        """Result dict must NOT contain 'alias' key when match was on label (not alias).

        Bug caught: storing alias=None in dict causes wd.get('alias', '') to return
        None, which crashes similarity(brand, None) with TypeError.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.side_effect = [
                _mock_get(_WBSEARCH_LABEL_MATCH),
                _mock_get(_SPARQL_RESPONSE_ONE),
            ]
            results = find_corporation("Apple")

        assert len(results) == 1
        assert "alias" not in results[0]
