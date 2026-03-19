"""Tests for pipeline/fetchers/wikidata.py — find_corporation."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest
import requests

import pipeline.fetchers.wikidata as _wd_module
from pipeline.fetchers.wikidata import (
    _search_entities,
    discover_brands_for_corporation,
    find_corporation,
    get_subsidiaries,
    query_sparql,
)


@pytest.fixture(autouse=True)
def reset_wikidata_cache(monkeypatch):
    """Reset in-process Wikidata cache before each test and suppress disk writes."""
    monkeypatch.setattr(_wd_module, "_CACHE", {"search_entities": {}, "subsidiaries": {}, "brands": {}})
    monkeypatch.setattr(_wd_module, "_save_cache", lambda: None)

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

    def test_search_entities_returns_label_in_result(self):
        """Each result dict contains a 'label' key with the Wikidata entity label.

        Bug caught: label key absent from return dict causes KeyError in
        enrich_corporation_qids when it calls hit.get('label', '') for
        similarity scoring.
        """
        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_WBSEARCH_LABEL_MATCH)
            results = _search_entities("Apple")

        assert len(results) == 1
        assert "label" in results[0], "label key must be present in _search_entities result"
        assert results[0]["label"] == "Apple Inc."


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


# ---------------------------------------------------------------------------
# discover_brands_for_corporation tests
# ---------------------------------------------------------------------------


class TestDiscoverBrandsForCorporation:
    def test_returns_brand_names_for_known_qid(self):
        """Returns list of {name, qid} dicts for entities linked via reverse P749.

        Bug caught: function fetches SPARQL results but fails to extract
        name/qid from bindings, returning empty list for all inputs.
        """
        binding = {
            "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q866"},
            "itemLabel": {"type": "literal", "value": "YouTube"},
        }
        with patch("pipeline.fetchers.wikidata.query_sparql", return_value=[binding]):
            results = discover_brands_for_corporation("Q95")

        assert results == [{"name": "YouTube", "qid": "Q866"}]

    def test_sparql_contains_q4830453_filter(self):
        """SPARQL query must include FILTER NOT EXISTS for Q4830453 (business enterprise).

        The filter runs server-side so we can only verify the query string is
        correctly constructed. Without this filter, subsidiary Corporation nodes
        would leak into brand results, duplicating the subsidiary discovery work.
        """
        captured = {}

        def capture_sparql(sparql):
            captured["query"] = sparql
            return []

        with patch("pipeline.fetchers.wikidata.query_sparql", side_effect=capture_sparql):
            discover_brands_for_corporation("Q95")

        assert "Q4830453" in captured["query"], (
            "SPARQL query must filter out Q4830453 (business enterprise) instances "
            "to avoid returning subsidiary corporations"
        )

    def test_returns_empty_list_when_no_results(self):
        """Returns [] without error when SPARQL returns no bindings.

        Bug caught: function crashes (IndexError) when iterating empty results.
        """
        with patch("pipeline.fetchers.wikidata.query_sparql", return_value=[]):
            results = discover_brands_for_corporation("Q95")

        assert results == []

    def test_skips_binding_with_missing_label(self):
        """Bindings with missing or empty itemLabel are excluded from results.

        Bug caught: Wikidata sometimes returns entities with no English label.
        Storing empty-name brands pollutes the graph with useless Brand nodes.
        """
        binding = {
            "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q999"},
            # no itemLabel key
        }
        with patch("pipeline.fetchers.wikidata.query_sparql", return_value=[binding]):
            results = discover_brands_for_corporation("Q95")

        assert results == []

    def test_warns_when_result_count_hits_limit(self):
        """Emits logger.warning when results hit the LIMIT threshold.

        Bug caught: silent truncation — corps with 200+ child entities get
        partial results with no indication that data is missing.
        """
        from pipeline.fetchers.wikidata import _BRAND_DISCOVERY_LIMIT

        bindings = [
            {
                "item": {"type": "uri", "value": f"http://www.wikidata.org/entity/Q{i}"},
                "itemLabel": {"type": "literal", "value": f"Brand{i}"},
            }
            for i in range(_BRAND_DISCOVERY_LIMIT)
        ]

        with (
            patch("pipeline.fetchers.wikidata.query_sparql", return_value=bindings),
            patch("pipeline.fetchers.wikidata.logger") as mock_logger,
        ):
            discover_brands_for_corporation("Q95")

        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "LIMIT" in warning_msg or "truncat" in warning_msg.lower()

    def test_rejects_invalid_qid_format(self):
        """Returns [] immediately for QIDs that do not start with Q.

        Bug caught: malformed QID (e.g. a URI fragment or empty string) is
        interpolated directly into SPARQL, producing a syntax error query that
        causes Wikidata to return HTTP 400/500.
        """
        with patch("pipeline.fetchers.wikidata.query_sparql") as mock_sparql:
            result_empty = discover_brands_for_corporation("")
            result_invalid = discover_brands_for_corporation("invalid-no-q")
            result_none = discover_brands_for_corporation(None)

        mock_sparql.assert_not_called()
        assert result_empty == []
        assert result_invalid == []
        assert result_none == []


# ---------------------------------------------------------------------------
# Tests: disk-backed cache
# ---------------------------------------------------------------------------


class TestCaching:
    def test_search_entities_cache_hit_skips_http(self):
        """Cache hit for _search_entities must not make any HTTP request."""
        cached = [{"qid": "Q312", "matched_alias": None, "label": "Apple Inc."}]
        _wd_module._CACHE["search_entities"]["Apple"] = cached

        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            result = _search_entities("Apple")

        mock_get.assert_not_called()
        assert result == cached

    def test_search_entities_cache_miss_saves_result(self):
        """Cache miss for _search_entities makes HTTP call and populates cache."""
        saved = {}
        _wd_module._save_cache = lambda: None  # already patched by fixture

        def capture_save():
            saved.update(_wd_module._CACHE["search_entities"])

        _wd_module._save_cache = capture_save

        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            mock_get.return_value = _mock_get(_WBSEARCH_RESPONSE_ONE)
            result = _search_entities("Procter & Gamble")

        mock_get.assert_called_once()
        assert "Procter & Gamble" in saved
        assert result[0]["qid"] == "Q185741"

    def test_get_subsidiaries_cache_hit_skips_sparql(self):
        """Cache hit for get_subsidiaries must not execute a SPARQL query."""
        cached = [{"qid": "Q866", "name": "YouTube"}]
        _wd_module._CACHE["subsidiaries"]["Q95"] = cached

        with patch("pipeline.fetchers.wikidata.query_sparql") as mock_sparql:
            result = get_subsidiaries("Q95")

        mock_sparql.assert_not_called()
        assert result == cached

    def test_get_subsidiaries_cache_miss_saves_result(self):
        """Cache miss for get_subsidiaries calls SPARQL and caches the result."""
        binding = {
            "subsidiary": {"type": "uri", "value": "http://www.wikidata.org/entity/Q866"},
            "subsidiaryLabel": {"type": "literal", "value": "YouTube"},
        }
        with patch("pipeline.fetchers.wikidata.query_sparql", return_value=[binding]):
            result = get_subsidiaries("Q95")

        assert result == [{"qid": "Q866", "name": "YouTube"}]
        assert _wd_module._CACHE["subsidiaries"]["Q95"] == result

    def test_discover_brands_cache_hit_skips_sparql(self):
        """Cache hit for discover_brands_for_corporation must not execute SPARQL."""
        cached = [{"name": "YouTube", "qid": "Q866"}]
        _wd_module._CACHE["brands"]["Q95"] = cached

        with patch("pipeline.fetchers.wikidata.query_sparql") as mock_sparql:
            result = discover_brands_for_corporation("Q95")

        mock_sparql.assert_not_called()
        assert result == cached

    def test_discover_brands_cache_miss_saves_result(self):
        """Cache miss for discover_brands_for_corporation calls SPARQL and caches."""
        binding = {
            "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q866"},
            "itemLabel": {"type": "literal", "value": "YouTube"},
        }
        with patch("pipeline.fetchers.wikidata.query_sparql", return_value=[binding]):
            result = discover_brands_for_corporation("Q95")

        assert result == [{"name": "YouTube", "qid": "Q866"}]
        assert _wd_module._CACHE["brands"]["Q95"] == result


# ---------------------------------------------------------------------------
# Tests: _search_entities retry on 429
# ---------------------------------------------------------------------------


class TestSearchEntitiesRetry:
    def test_search_entities_retries_on_429(self):
        """_search_entities must retry on 429 Too Many Requests and succeed."""
        ok_resp = _mock_get(_WBSEARCH_RESPONSE_ONE)
        err_resp = MagicMock(raise_for_status=MagicMock(side_effect=requests.HTTPError("429")))
        # Make err_resp look retryable
        err_resp.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")

        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            with patch("pipeline.fetchers.wikidata.time.sleep"):
                mock_get.side_effect = [err_resp, ok_resp]
                results = _search_entities("Procter & Gamble")

        assert len(results) == 1
        assert mock_get.call_count == 2

    def test_search_entities_does_not_retry_on_400(self):
        """_search_entities must not retry on 400 Bad Request."""
        err_resp = MagicMock(raise_for_status=MagicMock(side_effect=requests.HTTPError("400")))

        with patch("pipeline.fetchers.wikidata.requests.get") as mock_get:
            with patch("pipeline.fetchers.wikidata.time.sleep") as mock_sleep:
                mock_get.return_value = err_resp
                try:
                    _search_entities("bad query")
                except requests.HTTPError:
                    pass

        assert mock_get.call_count == 1
        mock_sleep.assert_not_called()
