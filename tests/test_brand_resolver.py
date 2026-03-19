"""Tests for pipeline/processors/brand_resolver.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from pipeline.processors.brand_resolver import resolve_all_brands, resolve_brand

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

GOOD_WD_MATCH = [{"qid": "Q312", "name": "Apple Inc.", "source": "wikidata"}]
GOOD_OC_MATCH = [{"name": "Mars, Inc.", "opencorporates_url": "https://opencorporates.com/companies/us_de/123", "jurisdiction_code": "us_de"}]


# ---------------------------------------------------------------------------
# resolve_all_brands — caching behaviour
# ---------------------------------------------------------------------------


class TestResolveAllBrandsCaching:
    def test_resolve_all_brands_skips_cached_brands(self, tmp_path):
        """Re-running pipeline must not re-query Wikidata for already-resolved brands.

        Bug caught: without cache-skip, every re-run exhausts Wikidata quota
        by re-querying brands that already resolved on a previous run.
        """
        cache_file = tmp_path / "brand_resolutions.json"
        cache_file.write_text(json.dumps({"Apple": {"name": "Apple Inc.", "qid": "Q312"}}))

        with patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd:
            mock_wd.return_value = GOOD_WD_MATCH
            resolve_all_brands(["Apple", "Samsung"], cache_file)

        # find_corporation called only once — for Samsung, not Apple
        mock_wd.assert_called_once_with("Samsung")

    def test_resolve_all_brands_skips_brands_cached_as_none(self, tmp_path):
        """Brands cached as null (tried, no match) must not be re-queried.

        Bug caught: without null caching, every re-run wastes quota on brands
        that have no Wikidata or OC entry.
        """
        cache_file = tmp_path / "brand_resolutions.json"
        cache_file.write_text(json.dumps({"SC Johnson": None}))

        with patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd:
            mock_wd.return_value = []
            resolve_all_brands(["SC Johnson"], cache_file)

        mock_wd.assert_not_called()

    def test_resolve_all_brands_saves_cache_after_each_brand(self, tmp_path):
        """Cache must be written after each brand, not once at the end.

        Bug caught: all-or-nothing save loses all results when pipeline is
        killed mid-run (e.g. OOM, SIGTERM, network cut).
        """
        cache_file = tmp_path / "brand_resolutions.json"

        with patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd, \
             patch("pipeline.processors.brand_resolver._save_cache") as mock_save:
            mock_wd.return_value = GOOD_WD_MATCH
            resolve_all_brands(["Apple", "Samsung"], cache_file)

        # _save_cache must be called once per brand (2 brands → 2 saves)
        assert mock_save.call_count == 2

    def test_resolve_all_brands_returns_only_successful_resolutions(self, tmp_path):
        """None entries must not appear in the returned dict.

        Bug caught: None entries in the return value break downstream loaders
        that iterate over the dict and call load_corporations().
        """
        cache_file = tmp_path / "brand_resolutions.json"

        with patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd, \
             patch("pipeline.processors.brand_resolver.search_companies") as mock_oc:
            mock_wd.side_effect = [GOOD_WD_MATCH, []]  # Apple resolves, Mars doesn't
            mock_oc.return_value = []
            result = resolve_all_brands(["Apple", "Mars"], cache_file)

        assert "Apple" in result
        assert "Mars" not in result
        assert len(result) == 1


# ---------------------------------------------------------------------------
# resolve_brand — Wikidata-primary, OC fallback
# ---------------------------------------------------------------------------


class TestResolveBrand:
    def test_resolve_brand_uses_wikidata_first(self):
        """OC must not be called when Wikidata returns a confident match.

        Bug caught: unconditional OC calls exhaust the 50 req/day free tier
        even when Wikidata already resolved the brand.
        """
        with patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd, \
             patch("pipeline.processors.brand_resolver.search_companies") as mock_oc:
            mock_wd.return_value = GOOD_WD_MATCH  # Apple Inc. matches "Apple" >= 0.7
            result = resolve_brand("Apple", oc_calls_used=[0])

        mock_oc.assert_not_called()
        assert result is not None

    def test_resolve_brand_falls_back_to_oc_when_wikidata_returns_no_match(self):
        """Brands with poor Wikidata coverage must resolve via OC fallback.

        Bug caught: without OC fallback, private companies like Mars and
        SC Johnson never resolve because Wikidata data is sparse.
        """
        oc_calls_used = [0]
        with patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd, \
             patch("pipeline.processors.brand_resolver.search_companies") as mock_oc:
            mock_wd.return_value = []
            mock_oc.return_value = GOOD_OC_MATCH
            result = resolve_brand("Mars", oc_calls_used=oc_calls_used, max_oc_calls=20)

        assert result is not None
        assert oc_calls_used[0] == 1

    def test_resolve_brand_skips_oc_when_quota_exhausted(self):
        """OC must not be called once max_oc_calls is reached.

        Bug caught: exceeding the 50 req/day free tier triggers HTTP 403,
        which breaks the rest of the pipeline run.
        """
        with patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd, \
             patch("pipeline.processors.brand_resolver.search_companies") as mock_oc:
            mock_wd.return_value = []
            result = resolve_brand("Mars", oc_calls_used=[20], max_oc_calls=20)

        mock_oc.assert_not_called()
        assert result is None
