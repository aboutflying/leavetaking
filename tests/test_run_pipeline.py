"""Tests for pipeline/run_pipeline.py — wiring of FEC data pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# All patches target the run_pipeline module namespace (where names are imported)
_PATCH_BASE = "pipeline.run_pipeline"


def _make_session():
    return MagicMock()


def _mock_path():
    p = MagicMock(spec=Path)
    return p


@pytest.fixture
def fec_patches():
    """Patch all FEC download/parse/load functions and settings for run_fec tests."""
    with (
        patch(f"{_PATCH_BASE}.settings") as mock_settings,
        patch(f"{_PATCH_BASE}.download_bulk_file", return_value=_mock_path()) as mock_dl,
        patch(f"{_PATCH_BASE}.parse_committee_master", return_value=iter([])) as mock_cm,
        patch(f"{_PATCH_BASE}.parse_candidate_master", return_value=iter([])) as mock_cn,
        patch(f"{_PATCH_BASE}.parse_committee_contributions", return_value=iter([])) as mock_pas2,
        patch(
            f"{_PATCH_BASE}.parse_candidate_committee_linkage", return_value=iter([])
        ) as mock_ccl,
        patch(f"{_PATCH_BASE}.load_candidates") as mock_load_cands,
        patch(f"{_PATCH_BASE}.load_committees") as mock_load_cmtes,
        patch(f"{_PATCH_BASE}.load_committee_contributions") as mock_load_contribs,
        patch(f"{_PATCH_BASE}.load_candidate_committee_linkage") as mock_load_linkage,
        patch(f"{_PATCH_BASE}.filter_corporate_pacs", return_value=[]) as mock_filter_pacs,
        patch(f"{_PATCH_BASE}.is_fec_cycle_loaded", return_value=False) as mock_is_loaded,
        patch(f"{_PATCH_BASE}.mark_fec_cycle_loaded") as mock_mark_loaded,
    ):
        mock_settings.fec_cycles = [2026]
        yield {
            "settings": mock_settings,
            "download_bulk_file": mock_dl,
            "parse_committee_master": mock_cm,
            "parse_candidate_master": mock_cn,
            "parse_committee_contributions": mock_pas2,
            "parse_candidate_committee_linkage": mock_ccl,
            "load_candidates": mock_load_cands,
            "load_committees": mock_load_cmtes,
            "load_committee_contributions": mock_load_contribs,
            "load_candidate_committee_linkage": mock_load_linkage,
            "filter_corporate_pacs": mock_filter_pacs,
            "is_fec_cycle_loaded": mock_is_loaded,
            "mark_fec_cycle_loaded": mock_mark_loaded,
        }


def test_run_fec_skips_already_loaded_cycle(fec_patches):
    """Cycles with a FECCycleLoad marker are skipped without downloading."""
    from pipeline.run_pipeline import run_fec

    fec_patches["is_fec_cycle_loaded"].return_value = True
    run_fec(_make_session())

    fec_patches["download_bulk_file"].assert_not_called()
    fec_patches["load_candidates"].assert_not_called()


def test_run_fec_force_bypasses_loaded_check(fec_patches):
    """force=True loads the cycle even when the marker exists."""
    from pipeline.run_pipeline import run_fec

    fec_patches["is_fec_cycle_loaded"].return_value = True
    run_fec(_make_session(), force=True)

    assert fec_patches["download_bulk_file"].called


def test_run_fec_marks_cycle_loaded_after_completion(fec_patches):
    """A FECCycleLoad marker is written after a cycle completes successfully."""
    from pipeline.run_pipeline import run_fec

    run_fec(_make_session())

    fec_patches["mark_fec_cycle_loaded"].assert_called_once()
    call_args = fec_patches["mark_fec_cycle_loaded"].call_args
    assert call_args[0][1] == 2026  # second positional arg is the cycle


def test_run_fec_does_not_download_indiv(fec_patches):
    """run_fec must not download the indiv (individual contributions) file."""
    from pipeline.run_pipeline import run_fec

    run_fec(_make_session())

    called_file_types = [c[0][0] for c in fec_patches["download_bulk_file"].call_args_list]
    assert "indiv" not in called_file_types, (
        f"indiv was downloaded — exec donations are out of scope for this epic. "
        f"download_bulk_file was called with: {called_file_types}"
    )


def test_run_fec_downloads_ccl(fec_patches):
    """run_fec must download the ccl (candidate-committee linkage) file."""
    from pipeline.run_pipeline import run_fec

    run_fec(_make_session())

    called_file_types = [c[0][0] for c in fec_patches["download_bulk_file"].call_args_list]
    assert "ccl" in called_file_types, (
        f"ccl was not downloaded — ccl26 linkage is required. "
        f"download_bulk_file was called with: {called_file_types}"
    )


def test_run_fec_applies_transaction_type_filter(fec_patches):
    """run_fec must filter pas2 contributions to 24K/24Z before loading."""
    from pipeline.run_pipeline import run_fec

    rows_with_mixed_types = [
        {
            "transaction_type": "24K",
            "transaction_amount": "1000",
            "committee_id": "C001",
            "candidate_id": "P001",
            "transaction_id": "TXN1",
            "transaction_date": "01012026",
        },
        {
            "transaction_type": "24A",
            "transaction_amount": "500",
            "committee_id": "C001",
            "candidate_id": "P001",
            "transaction_id": "TXN2",
            "transaction_date": "01012026",
        },
    ]
    fec_patches["parse_committee_contributions"].return_value = iter(rows_with_mixed_types)

    # Capture what load_committee_contributions actually receives
    loaded_rows = []
    fec_patches["load_committee_contributions"].side_effect = lambda session, rows: (
        loaded_rows.extend(rows)
    )

    run_fec(_make_session())

    transaction_types = [r["transaction_type"] for r in loaded_rows]
    assert "24A" not in transaction_types, (
        "Opposition expenditure 24A must be filtered out before loading"
    )
    assert "24K" in transaction_types, "Direct support 24K must be passed through to loading"


class TestRunBrands:
    """Tests for run_brands() — brand resolution + Neo4j loading."""

    def _make_brands_patches(self):
        return (
            patch(f"{_PATCH_BASE}.resolve_all_brands"),
            patch(f"{_PATCH_BASE}.get_ownership_chain", return_value=[]),
            patch(f"{_PATCH_BASE}.load_brands"),
            patch(f"{_PATCH_BASE}.load_corporations"),
            patch(f"{_PATCH_BASE}.load_ownership_edges"),
            patch(f"{_PATCH_BASE}.load_subsidiary_edges"),
            patch(f"{_PATCH_BASE}.settings"),
        )

    def test_run_brands_calls_resolve_all_brands_with_cache_path_in_data_dir(self):
        """run_brands must pass a cache path inside settings.data_dir.

        Bug caught: wrong cache path means the pipeline never finds previously
        cached resolutions, re-querying Wikidata on every run.
        """
        from pipeline.run_pipeline import run_brands

        with (
            patch(f"{_PATCH_BASE}.resolve_all_brands", return_value={}) as mock_resolve,
            patch(f"{_PATCH_BASE}.get_ownership_chain", return_value=[]),
            patch(f"{_PATCH_BASE}.load_brands"),
            patch(f"{_PATCH_BASE}.load_corporations"),
            patch(f"{_PATCH_BASE}.load_ownership_edges"),
            patch(f"{_PATCH_BASE}.load_subsidiary_edges"),
            patch(f"{_PATCH_BASE}.settings") as mock_settings,
        ):
            mock_settings.data_dir = Path("/fake/data")
            run_brands(_make_session())

        mock_resolve.assert_called_once()
        cache_path = mock_resolve.call_args[0][1]
        assert str(cache_path).endswith("brand_resolutions.json"), (
            f"Expected cache path ending in brand_resolutions.json, got: {cache_path}"
        )
        assert str(cache_path).startswith("/fake/data"), (
            f"Cache path must be inside settings.data_dir, got: {cache_path}"
        )

    def test_run_brands_loads_corporations_and_ownership_edges(self):
        """Resolved brands must be loaded into Neo4j as Corporation + OWNED_BY edges.

        Bug caught: brand resolution succeeds but no Neo4j writes happen,
        leaving the graph empty and PAC linkage unable to find any corporations.
        """
        from pipeline.run_pipeline import run_brands

        with (
            patch(
                f"{_PATCH_BASE}.resolve_all_brands",
                return_value={"Apple": {"name": "Apple Inc.", "qid": "Q312", "ticker": "AAPL"}},
            ),
            patch(f"{_PATCH_BASE}.get_ownership_chain", return_value=[]),
            patch(f"{_PATCH_BASE}.load_brands"),
            patch(f"{_PATCH_BASE}.load_corporations") as mock_load_corps,
            patch(f"{_PATCH_BASE}.load_ownership_edges") as mock_load_own,
            patch(f"{_PATCH_BASE}.load_subsidiary_edges"),
            patch(f"{_PATCH_BASE}.settings") as mock_settings,
        ):
            mock_settings.data_dir = Path("/fake/data")
            run_brands(_make_session())

        corp_names = [c["name"] for c in mock_load_corps.call_args[0][1]]
        assert "Apple Inc." in corp_names

        own_edges = mock_load_own.call_args[0][1]
        assert {"brand_name": "Apple", "corporation_name": "Apple Inc."} in own_edges

    def test_run_brands_skips_ownership_chain_when_no_qid(self):
        """get_ownership_chain must not be called when the resolution has no QID.

        Bug caught: calling get_ownership_chain(None) or get_ownership_chain('')
        sends a malformed Wikidata query and raises or returns garbage data.
        """
        from pipeline.run_pipeline import run_brands

        with (
            patch(
                f"{_PATCH_BASE}.resolve_all_brands", return_value={"X": {"name": "X Corp"}}
            ),  # no 'qid' key
            patch(f"{_PATCH_BASE}.get_ownership_chain") as mock_chain,
            patch(f"{_PATCH_BASE}.load_brands"),
            patch(f"{_PATCH_BASE}.load_corporations"),
            patch(f"{_PATCH_BASE}.load_ownership_edges"),
            patch(f"{_PATCH_BASE}.load_subsidiary_edges"),
            patch(f"{_PATCH_BASE}.settings") as mock_settings,
        ):
            mock_settings.data_dir = Path("/fake/data")
            run_brands(_make_session())

        mock_chain.assert_not_called()


def test_run_fec_passes_known_cand_ids_to_linkage_loader(fec_patches):
    """run_fec must pass known_cand_ids (not None) to load_candidate_committee_linkage."""
    from pipeline.run_pipeline import run_fec

    # parse_candidate_master returns a candidate with a known ID
    fec_patches["parse_candidate_master"].return_value = iter(
        [
            {
                "candidate_id": "P00000001",
                "candidate_name": "Test Candidate",
                "party": "DEM",
                "election_year": "2026",
                "office_state": "CA",
                "office": "H",
                "office_district": "01",
                "incumbent_challenger_status": "I",
                "candidate_status": "C",
                "principal_committee_id": "C001",
                "street1": "",
                "street2": "",
                "city": "",
                "state": "",
                "zip": "",
            },
        ]
    )

    run_fec(_make_session())

    assert fec_patches["load_candidate_committee_linkage"].called, (
        "load_candidate_committee_linkage was not called"
    )
    call_kwargs = fec_patches["load_candidate_committee_linkage"].call_args[1]
    assert "known_cand_ids" in call_kwargs, (
        "known_cand_ids kwarg not passed — validation will be skipped"
    )
    assert call_kwargs["known_cand_ids"] is not None, "known_cand_ids must be a set, not None"
    assert isinstance(call_kwargs["known_cand_ids"], set), (
        f"known_cand_ids must be a set, got {type(call_kwargs['known_cand_ids'])}"
    )


# ---------------------------------------------------------------------------
# enrich_corporation_qids tests
# ---------------------------------------------------------------------------

from pipeline.run_pipeline import enrich_corporation_qids  # noqa: E402


class TestEnrichCorporationQids:
    def test_no_work_when_all_corps_have_qids(self):
        """Returns 0 and calls no API when Neo4j returns no QID-less corps.

        Bug caught: function iterates all corps instead of relying on
        WHERE c.qid IS NULL filter, wasting API quota on re-runs.
        """
        session = _make_session()
        session.run.return_value = []  # WHERE c.qid IS NULL returns nothing

        with patch(f"{_PATCH_BASE}._search_entities") as mock_search:
            result = enrich_corporation_qids(session, delay=0)

        mock_search.assert_not_called()
        assert result == 0

    def test_writes_qid_for_corp_above_similarity_threshold(self):
        """Writes QID to Corporation node when similarity(name, label) >= 0.7.

        Bug caught: function finds match via wbsearchentities but never calls
        load_corporations to persist the QID — silently returns 0.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Apple Inc."}]

        with (
            patch(f"{_PATCH_BASE}._search_entities") as mock_search,
            patch(f"{_PATCH_BASE}.load_corporations") as mock_load,
        ):
            mock_search.return_value = [
                {"qid": "Q312", "matched_alias": None, "label": "Apple Inc."}
            ]
            result = enrich_corporation_qids(session, delay=0)

        mock_load.assert_called_once_with(session, [{"name": "Apple Inc.", "qid": "Q312"}])
        assert result == 1

    def test_skips_low_similarity_match(self):
        """Does not write QID when similarity(name, label) < 0.7.

        Bug caught: accepts first wbsearchentities hit regardless of relevance,
        assigning a wrong QID (e.g. 'Apple Records' for 'Apple Inc.').
        """
        session = _make_session()
        session.run.return_value = [{"name": "Apple Inc."}]

        with (
            patch(f"{_PATCH_BASE}._search_entities") as mock_search,
            patch(f"{_PATCH_BASE}.load_corporations") as mock_load,
        ):
            mock_search.return_value = [
                {"qid": "Q9999", "matched_alias": None, "label": "Totally Different Corp XYZ"}
            ]
            result = enrich_corporation_qids(session, delay=0)

        mock_load.assert_not_called()
        assert result == 0

    def test_handles_empty_search_results(self):
        """Returns 0 without error when wbsearchentities returns no hits.

        Bug caught: function crashes (IndexError or KeyError) when iterating
        an empty hits list.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Acme Obscure Corp"}]

        with (
            patch(f"{_PATCH_BASE}._search_entities") as mock_search,
            patch(f"{_PATCH_BASE}.load_corporations") as mock_load,
        ):
            mock_search.return_value = []
            result = enrich_corporation_qids(session, delay=0)

        mock_load.assert_not_called()
        assert result == 0

    def test_handles_search_exception_and_continues(self):
        """HTTPError from _search_entities is caught; enrichment continues for other corps.

        Bug caught: unhandled exception aborts enrichment for all remaining
        corps when a single wbsearchentities call fails (e.g. 429 rate limit).
        """
        import requests

        session = _make_session()
        session.run.return_value = [{"name": "Acme"}, {"name": "Apple Inc."}]

        with (
            patch(f"{_PATCH_BASE}._search_entities") as mock_search,
            patch(f"{_PATCH_BASE}.load_corporations") as mock_load,
        ):
            mock_search.side_effect = [
                requests.HTTPError("429"),
                [{"qid": "Q312", "matched_alias": None, "label": "Apple Inc."}],
            ]
            result = enrich_corporation_qids(session, delay=0)

        # Apple Inc. should still be enriched despite Acme failing
        mock_load.assert_called_once()
        assert result == 1


# ---------------------------------------------------------------------------
# discover_subsidiaries_for_corpus tests
# ---------------------------------------------------------------------------

from pipeline.run_pipeline import discover_subsidiaries_for_corpus  # noqa: E402


class TestDiscoverSubsidiariesForCorpus:
    def test_skips_corporations_without_qid(self):
        """get_subsidiaries must not be called for Corps with no QID.

        Bug caught: calling get_subsidiaries(None) sends a malformed SPARQL
        query and returns garbage or raises.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Unknown Corp", "qid": None}]

        with patch(f"{_PATCH_BASE}.get_subsidiaries") as mock_subs:
            discover_subsidiaries_for_corpus(session, delay=0)

        mock_subs.assert_not_called()

    def test_creates_corporation_node_and_edge_for_each_subsidiary(self):
        """Each subsidiary returned by get_subsidiaries is loaded as a Corporation + SUBSIDIARY_OF edge.

        Bug caught: function fetches subsidiaries but never writes them to Neo4j,
        leaving the graph empty.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Alphabet Inc.", "qid": "Q95"}]

        with (
            patch(f"{_PATCH_BASE}.get_subsidiaries") as mock_subs,
            patch(f"{_PATCH_BASE}.load_corporations") as mock_load_corps,
            patch(f"{_PATCH_BASE}.load_subsidiary_edges") as mock_load_edges,
        ):
            mock_subs.return_value = [{"qid": "Q90003", "name": "Google LLC"}]
            discover_subsidiaries_for_corpus(session, delay=0)

        corp_names = [c["name"] for c in mock_load_corps.call_args[0][1]]
        assert "Google LLC" in corp_names

        edges = mock_load_edges.call_args[0][1]
        assert {"child_name": "Google LLC", "parent_name": "Alphabet Inc."} in edges

    def test_handles_get_subsidiaries_exception_and_continues(self):
        """Exception from get_subsidiaries for one corp does not abort discovery for others.

        Bug caught: unhandled exception stops the loop, leaving later corporations
        unenriched when one Wikidata call fails.
        """
        session = _make_session()
        session.run.return_value = [
            {"name": "BadCorp", "qid": "Q1"},
            {"name": "Alphabet Inc.", "qid": "Q95"},
        ]

        with (
            patch(f"{_PATCH_BASE}.get_subsidiaries") as mock_subs,
            patch(f"{_PATCH_BASE}.load_corporations") as mock_load_corps,
            patch(f"{_PATCH_BASE}.load_subsidiary_edges"),
        ):
            mock_subs.side_effect = [
                Exception("SPARQL timeout"),
                [{"qid": "Q90003", "name": "Google LLC"}],
            ]
            discover_subsidiaries_for_corpus(session, delay=0)

        mock_load_corps.assert_called_once()
        corp_names = [c["name"] for c in mock_load_corps.call_args[0][1]]
        assert "Google LLC" in corp_names

    def test_skips_subsidiaries_with_empty_name(self):
        """Subsidiaries with empty name are not loaded into Neo4j.

        Bug caught: Wikidata sometimes returns entities with empty labels
        (QID exists but no English label). Loading empty-name nodes pollutes
        the graph with useless Corporation nodes.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Alphabet Inc.", "qid": "Q95"}]

        with (
            patch(f"{_PATCH_BASE}.get_subsidiaries") as mock_subs,
            patch(f"{_PATCH_BASE}.load_corporations") as mock_load_corps,
        ):
            mock_subs.return_value = [{"qid": "Q999", "name": ""}]
            discover_subsidiaries_for_corpus(session, delay=0)

        mock_load_corps.assert_not_called()

    def test_returns_count_of_subsidiaries_loaded(self):
        """Return value is the total number of subsidiary Corporation nodes loaded.

        Bug caught: function always returns None, making it impossible to log
        or test how many subsidiaries were discovered.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Alphabet Inc.", "qid": "Q95"}]

        with (
            patch(f"{_PATCH_BASE}.get_subsidiaries") as mock_subs,
            patch(f"{_PATCH_BASE}.load_corporations"),
            patch(f"{_PATCH_BASE}.load_subsidiary_edges"),
        ):
            mock_subs.return_value = [
                {"qid": "Q90003", "name": "Google LLC"},
                {"qid": "Q312", "name": "YouTube"},
            ]
            result = discover_subsidiaries_for_corpus(session, delay=0)

        assert result == 2


# ---------------------------------------------------------------------------
# discover_brands_for_corpus tests
# ---------------------------------------------------------------------------

from pipeline.run_pipeline import discover_brands_for_corpus  # noqa: E402


class TestDiscoverBrandsForCorpus:
    def test_skips_corporations_without_qid(self):
        """discover_brands_for_corporation must not be called for Corps with no QID.

        Bug caught: calling discover_brands_for_corporation(None) sends invalid
        SPARQL and raises or returns garbage.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Unknown Corp", "qid": None}]

        with patch(f"{_PATCH_BASE}.discover_brands_for_corporation") as mock_discover:
            discover_brands_for_corpus(session, delay=0)

        mock_discover.assert_not_called()

    def test_creates_brand_node_and_owned_by_edge(self):
        """Each brand returned by discover_brands_for_corporation is loaded as Brand + OWNED_BY edge.

        Bug caught: function fetches brands but never calls load_brands or
        load_ownership_edges, leaving the graph empty.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Alphabet Inc.", "qid": "Q95"}]

        with (
            patch(
                f"{_PATCH_BASE}.discover_brands_for_corporation",
                return_value=[{"name": "YouTube", "qid": "Q866"}],
            ),
            patch(f"{_PATCH_BASE}.load_brands") as mock_load_brands,
            patch(f"{_PATCH_BASE}.load_ownership_edges") as mock_load_edges,
        ):
            discover_brands_for_corpus(session, delay=0)

        brand_names = [b["name"] for b in mock_load_brands.call_args[0][1]]
        assert "YouTube" in brand_names

        edges = mock_load_edges.call_args[0][1]
        assert {"brand_name": "YouTube", "corporation_name": "Alphabet Inc."} in edges

    def test_handles_discover_brands_exception_and_continues(self):
        """Exception from discover_brands_for_corporation does not abort the loop.

        Bug caught: unhandled exception stops corpus scan — remaining corporations
        get no brand discovery when one Wikidata call fails.
        """
        session = _make_session()
        session.run.return_value = [
            {"name": "BadCorp", "qid": "Q1"},
            {"name": "Alphabet Inc.", "qid": "Q95"},
        ]

        with (
            patch(f"{_PATCH_BASE}.discover_brands_for_corporation") as mock_discover,
            patch(f"{_PATCH_BASE}.load_brands") as mock_load_brands,
            patch(f"{_PATCH_BASE}.load_ownership_edges"),
        ):
            mock_discover.side_effect = [
                Exception("SPARQL timeout"),
                [{"name": "YouTube", "qid": "Q866"}],
            ]
            discover_brands_for_corpus(session, delay=0)

        mock_load_brands.assert_called_once()
        brand_names = [b["name"] for b in mock_load_brands.call_args[0][1]]
        assert "YouTube" in brand_names

    def test_skips_brands_with_empty_name(self):
        """Brands with empty name from discover_brands_for_corporation are not loaded.

        Bug caught: empty-name Brand nodes pollute the graph with useless nodes
        that cannot be matched to any Amazon product.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Alphabet Inc.", "qid": "Q95"}]

        with (
            patch(
                f"{_PATCH_BASE}.discover_brands_for_corporation",
                return_value=[{"name": "", "qid": "Q999"}],
            ),
            patch(f"{_PATCH_BASE}.load_brands") as mock_load_brands,
        ):
            discover_brands_for_corpus(session, delay=0)

        mock_load_brands.assert_not_called()

    def test_discovered_brands_pass_aliases_none(self):
        """Brands loaded by discover_brands_for_corpus must pass aliases=None, not [].

        Bug caught: passing aliases=[] triggers load_brands to overwrite existing
        aliases (if any) with an empty list. Passing None preserves existing aliases
        via the CASE guard added to load_brands.
        """
        session = _make_session()
        session.run.return_value = [{"name": "Alphabet Inc.", "qid": "Q95"}]

        with (
            patch(
                f"{_PATCH_BASE}.discover_brands_for_corporation",
                return_value=[{"name": "YouTube", "qid": "Q866"}],
            ),
            patch(f"{_PATCH_BASE}.load_brands") as mock_load_brands,
            patch(f"{_PATCH_BASE}.load_ownership_edges"),
        ):
            discover_brands_for_corpus(session, delay=0)

        loaded_brands = mock_load_brands.call_args[0][1]
        youtube_brand = next(b for b in loaded_brands if b["name"] == "YouTube")
        assert youtube_brand["aliases"] is None, (
            "Discovered brands must pass aliases=None so existing aliases are preserved "
            "by the CASE guard in load_brands"
        )


# ---------------------------------------------------------------------------
# deduplicate_corporations_by_qid tests
# ---------------------------------------------------------------------------

from pipeline.run_pipeline import deduplicate_corporations_by_qid  # noqa: E402


def _make_run_side_effect(*return_sequences):
    """Build a side_effect list for session.run that returns different values per call.

    Each arg is the return value for successive session.run calls.
    Wraps non-list values as iterables so the function can do `for r in result`.
    """
    return iter(return_sequences)


class TestDeduplicateCorporationsByQid:
    def test_returns_zero_when_no_duplicates(self):
        """Returns 0 and makes no writes when no QID has more than one Corporation.

        Bug caught: function always calls DELETE even when no duplicates exist,
        corrupting the graph on every run.
        """
        session = _make_session()
        # First call (find groups) returns empty — no duplicates
        session.run.return_value = []

        result = deduplicate_corporations_by_qid(session)

        assert result == 0
        # Only the group-finding query should have been called
        assert session.run.call_count == 1

    def test_picks_canonical_by_relationship_count(self):
        """Node with the most relationships becomes canonical; the other is deleted.

        Bug caught: function picks alphabetically-first node as canonical regardless
        of how many edges it has, causing the well-connected node to be deleted.
        """
        session = _make_session()

        # Call 1: find groups — one group with 2 names
        # Call 2: rank by rel_count — "Alphabet Inc." has 3, "Alphabet" has 1
        # Calls 3-8: re-home edges (4 MERGE queries + 1 alias SET + 1 DELETE)
        session.run.side_effect = [
            [{"qid": "Q95", "names": ["Alphabet Inc.", "Alphabet"]}],  # groups query
            [                                                             # ranked query
                {"name": "Alphabet Inc.", "aliases": [], "rel_count": 3},
                {"name": "Alphabet",      "aliases": [], "rel_count": 1},
            ],
            MagicMock(),  # re-home OWNED_BY
            MagicMock(),  # re-home SUBSIDIARY_OF child
            MagicMock(),  # re-home SUBSIDIARY_OF parent
            MagicMock(),  # re-home OPERATES_PAC
            MagicMock(),  # SET aliases
            MagicMock(),  # DETACH DELETE
        ]

        deduplicate_corporations_by_qid(session)

        # The DELETE call should be for "Alphabet" (the lower-rel-count node)
        delete_call = session.run.call_args_list[7]
        assert "Alphabet" in str(delete_call)
        assert "Alphabet Inc." not in str(delete_call).replace("canonical_name", "")

    def test_stores_duplicate_name_in_canonical_aliases(self):
        """Duplicate node's name is added to canonical.aliases before deletion.

        Bug caught: dedup deletes the duplicate but never stores its name as an
        alias, so edge loaders can no longer find the canonical by the old name.
        """
        session = _make_session()

        session.run.side_effect = [
            [{"qid": "Q95", "names": ["Alphabet Inc.", "Alphabet"]}],
            [
                {"name": "Alphabet Inc.", "aliases": [], "rel_count": 3},
                {"name": "Alphabet",      "aliases": [], "rel_count": 1},
            ],
            MagicMock(), MagicMock(), MagicMock(), MagicMock(),  # edge re-homes
            MagicMock(),  # SET aliases — we'll inspect this call
            MagicMock(),  # DETACH DELETE
        ]

        deduplicate_corporations_by_qid(session)

        # The 7th call (index 6) is the SET aliases call
        alias_call = session.run.call_args_list[6]
        alias_kwargs = alias_call[1]  # keyword args
        assert "Alphabet" in alias_kwargs.get("new_aliases", []), (
            "Duplicate name 'Alphabet' must appear in new_aliases passed to SET aliases query"
        )

    def test_returns_count_of_removed_nodes(self):
        """Return value equals the number of duplicate nodes removed.

        Bug caught: function always returns None, making it impossible to log
        or assert how many nodes were collapsed.
        """
        session = _make_session()

        # Two separate QID groups, each with one duplicate
        session.run.side_effect = [
            [
                {"qid": "Q95",  "names": ["Alphabet Inc.", "Alphabet"]},
                {"qid": "Q312", "names": ["Apple Inc.", "Apple"]},
            ],
            # Ranked for Q95
            [
                {"name": "Alphabet Inc.", "aliases": [], "rel_count": 3},
                {"name": "Alphabet",      "aliases": [], "rel_count": 1},
            ],
            MagicMock(), MagicMock(), MagicMock(), MagicMock(),  # edges Q95
            MagicMock(), MagicMock(),  # aliases + delete Q95
            # Ranked for Q312
            [
                {"name": "Apple Inc.", "aliases": [], "rel_count": 5},
                {"name": "Apple",      "aliases": [], "rel_count": 2},
            ],
            MagicMock(), MagicMock(), MagicMock(), MagicMock(),  # edges Q312
            MagicMock(), MagicMock(),  # aliases + delete Q312
        ]

        result = deduplicate_corporations_by_qid(session)

        assert result == 2

    def test_does_not_delete_canonical(self):
        """Only the non-canonical (lower-rel-count) node is DETACH DELETEd.

        Bug caught: both nodes deleted, leaving the QID entirely absent from graph.
        """
        session = _make_session()

        session.run.side_effect = [
            [{"qid": "Q95", "names": ["Alphabet Inc.", "Alphabet"]}],
            [
                {"name": "Alphabet Inc.", "aliases": [], "rel_count": 3},
                {"name": "Alphabet",      "aliases": [], "rel_count": 1},
            ],
            MagicMock(), MagicMock(), MagicMock(), MagicMock(),
            MagicMock(),  # SET aliases
            MagicMock(),  # DETACH DELETE
        ]

        deduplicate_corporations_by_qid(session)

        all_queries = [str(c) for c in session.run.call_args_list]
        delete_queries = [q for q in all_queries if "DETACH DELETE" in q]
        assert len(delete_queries) == 1, "Exactly one DETACH DELETE expected"
        # The one DELETE must reference the dup name, not the canonical
        assert "Alphabet Inc." not in delete_queries[0].replace("canonical_name", "")

    def test_includes_dup_existing_aliases_in_transfer(self):
        """If the duplicate already has aliases, those are transferred to canonical too.

        Bug caught: only dup.name is transferred; dup.aliases are silently lost,
        so edge loaders that previously resolved via dup's aliases now break.
        """
        session = _make_session()

        session.run.side_effect = [
            [{"qid": "Q95", "names": ["Alphabet Inc.", "Alphabet"]}],
            [
                {"name": "Alphabet Inc.", "aliases": [],                     "rel_count": 3},
                {"name": "Alphabet",      "aliases": ["Alphabet Holdings"],  "rel_count": 1},
            ],
            MagicMock(), MagicMock(), MagicMock(), MagicMock(),
            MagicMock(),  # SET aliases
            MagicMock(),  # DETACH DELETE
        ]

        deduplicate_corporations_by_qid(session)

        alias_call = session.run.call_args_list[6]
        alias_kwargs = alias_call[1]
        new_aliases = alias_kwargs.get("new_aliases", [])
        assert "Alphabet" in new_aliases
        assert "Alphabet Holdings" in new_aliases
