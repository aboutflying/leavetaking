"""Tests for pipeline/processors/brand_resolver.py."""

from __future__ import annotations

import json
from unittest.mock import patch


from pipeline.processors.brand_resolver import _stdin_prompt, resolve_all_brands, resolve_brand

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

GOOD_WD_MATCH = [{"qid": "Q312", "name": "Apple Inc.", "source": "wikidata"}]
GOOD_OC_MATCH = [
    {
        "name": "Mars, Inc.",
        "opencorporates_url": "https://opencorporates.com/companies/us_de/123",
        "jurisdiction_code": "us_de",
    }
]


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

        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver._save_cache") as mock_save,
        ):
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

        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver.search_companies") as mock_oc,
        ):
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
        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver.search_companies") as mock_oc,
        ):
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
        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver.search_companies") as mock_oc,
        ):
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
        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver.search_companies") as mock_oc,
        ):
            mock_wd.return_value = []
            result = resolve_brand("Mars", oc_calls_used=[20], max_oc_calls=20)

        mock_oc.assert_not_called()
        assert result is None


# ---------------------------------------------------------------------------
# resolve_brand — prompt_fn callback
# ---------------------------------------------------------------------------

# A Wikidata candidate that scores below 0.7 against "Bose"
_BELOW_THRESHOLD_WD = [{"name": "Bose-Einstein Condensate Research", "qid": "Q999"}]
# A Wikidata candidate that scores >= 0.7 against "Apple"
_ABOVE_THRESHOLD_WD = [{"name": "Apple Inc.", "qid": "Q312"}]


class TestResolveBrandPromptFn:
    def test_prompt_fn_called_when_below_threshold_and_result_used(self):
        """prompt_fn is called with candidates and its return value becomes the result.

        Bug caught: prompt_fn called but return value ignored — user picks
        a candidate but resolve_brand still returns None.
        """
        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver.search_companies") as mock_oc,
        ):
            mock_wd.return_value = _BELOW_THRESHOLD_WD
            mock_oc.return_value = []
            prompt_fn = lambda name, cands: cands[0]
            result = resolve_brand("Bose", oc_calls_used=[0], prompt_fn=prompt_fn)

        assert result is not None
        assert result["qid"] == "Q999"

    def test_prompt_fn_skip_returns_none(self):
        """prompt_fn returning None records brand as unresolved (user skipped).

        Bug caught: skip choice not propagated — resolve_brand returns a
        candidate despite the user choosing to skip.
        """
        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver.search_companies") as mock_oc,
        ):
            mock_wd.return_value = _BELOW_THRESHOLD_WD
            mock_oc.return_value = []
            result = resolve_brand("Bose", oc_calls_used=[0], prompt_fn=lambda n, c: None)

        assert result is None

    def test_prompt_fn_none_preserves_headless_behavior(self):
        """prompt_fn=None (default) returns None below threshold without raising.

        Bug caught: regression in headless behavior — pipeline crashes when
        prompt_fn is not provided.
        """
        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver.search_companies") as mock_oc,
        ):
            mock_wd.return_value = _BELOW_THRESHOLD_WD
            mock_oc.return_value = []
            result = resolve_brand("Bose", oc_calls_used=[0])  # prompt_fn defaults to None

        assert result is None

    def test_prompt_fn_not_called_when_no_candidates(self):
        """prompt_fn must not be called when both WD and OC return no results.

        Bug caught: prompt shown with empty candidates list, causing IndexError
        or confusing UX.
        """
        from unittest.mock import Mock

        prompt_fn = Mock()
        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver.search_companies") as mock_oc,
        ):
            mock_wd.return_value = []
            mock_oc.return_value = []
            resolve_brand("Bose", oc_calls_used=[0], prompt_fn=prompt_fn)

        prompt_fn.assert_not_called()

    def test_prompt_fn_not_called_when_auto_match_succeeds(self):
        """prompt_fn must not be called when Wikidata returns a confident match.

        Bug caught: prompt shown unnecessarily, interrupting batch run for
        brands that already resolved automatically.
        """
        from unittest.mock import Mock

        prompt_fn = Mock()
        with patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd:
            mock_wd.return_value = _ABOVE_THRESHOLD_WD
            resolve_brand("Apple", oc_calls_used=[0], prompt_fn=prompt_fn)

        prompt_fn.assert_not_called()

    def test_prompt_fn_exception_propagates(self):
        """Exceptions raised by prompt_fn are not swallowed.

        Bug caught: hidden errors in prompt callbacks cause silent data loss
        instead of a clear failure.
        """
        with (
            patch("pipeline.processors.brand_resolver.find_corporation") as mock_wd,
            patch("pipeline.processors.brand_resolver.search_companies") as mock_oc,
        ):
            mock_wd.return_value = _BELOW_THRESHOLD_WD
            mock_oc.return_value = []

            def exploding_prompt(name, cands):
                raise RuntimeError("prompt error")

            import pytest
            with pytest.raises(RuntimeError, match="prompt error"):
                resolve_brand("Bose", oc_calls_used=[0], prompt_fn=exploding_prompt)


# ---------------------------------------------------------------------------
# _stdin_prompt — terminal UI
# ---------------------------------------------------------------------------

_PROMPT_CANDIDATES = [
    {"name": "Bose Corporation", "score": 0.61, "source": "wikidata", "qid": "Q174959"},
    {"name": "Bose Audio GmbH", "score": 0.44, "source": "opencorporates",
     "jurisdiction": "de", "company_number": "456"},
]


class TestStdinPrompt:
    def test_returns_first_candidate_on_choice_1(self):
        """Typing '1' returns the first (highest-scoring) candidate.

        Bug caught: _stdin_prompt uses wrong index or ignores input entirely.
        """
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch("builtins.input", return_value="1"),
        ):
            result = _stdin_prompt("Bose", _PROMPT_CANDIDATES)
        assert result is _PROMPT_CANDIDATES[0]

    def test_returns_none_on_skip(self):
        """Typing 's' records brand as unresolved (user skipped).

        Bug caught: skip choice not honored — returns a candidate instead of None.
        """
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch("builtins.input", return_value="s"),
        ):
            result = _stdin_prompt("Bose", _PROMPT_CANDIDATES)
        assert result is None

    def test_returns_none_without_calling_input_when_not_tty(self):
        """When stdin is not a TTY, returns None immediately without calling input().

        Bug caught: pipeline blocks on input() in cron/CI where stdin is a pipe.
        """
        from unittest.mock import Mock
        mock_input = Mock()
        with (
            patch("sys.stdin.isatty", return_value=False),
            patch("builtins.input", mock_input),
        ):
            result = _stdin_prompt("Bose", _PROMPT_CANDIDATES)
        assert result is None
        mock_input.assert_not_called()

    def test_reprompts_on_invalid_input_then_succeeds(self):
        """Invalid choices ('99', '0', 'x') cause re-prompt; valid '1' succeeds.

        Bug caught: invalid input crashes with ValueError or IndexError instead
        of printing an error and asking again.
        """
        from unittest.mock import Mock, call
        mock_input = Mock(side_effect=["99", "0", "x", "1"])
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch("builtins.input", mock_input),
        ):
            result = _stdin_prompt("Bose", _PROMPT_CANDIDATES)
        assert result is _PROMPT_CANDIDATES[0]
        assert mock_input.call_count == 4

    def test_returns_none_on_eof_without_raising(self):
        """EOFError from input() returns None instead of propagating.

        Bug caught: pipeline crashes mid-batch when stdin closes (remote session
        drop, piped file ends).
        """
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch("builtins.input", side_effect=EOFError),
        ):
            result = _stdin_prompt("Bose", _PROMPT_CANDIDATES)
        assert result is None

    def test_resolve_all_brands_default_prompt_fn_is_stdin_prompt(self):
        """resolve_all_brands default prompt_fn is _stdin_prompt, not None.

        Bug caught: resolve_all_brands defaults to prompt_fn=None, so the
        pipeline never prompts even when brands fall below threshold.

        Uses inspect.signature to verify the wiring at definition time —
        patching the module-level name wouldn't work because default arguments
        are evaluated when the function is defined, not when it's called.
        """
        import inspect
        sig = inspect.signature(resolve_all_brands)
        default = sig.parameters["prompt_fn"].default
        assert default is _stdin_prompt, (
            f"resolve_all_brands prompt_fn default should be _stdin_prompt, got {default!r}"
        )
