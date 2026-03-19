"""Tests for pipeline/processors/entity_resolution.py."""

from __future__ import annotations

from pipeline.processors.entity_resolution import (
    _get_scored_candidates,
    filter_corporate_pacs,
    filter_supported_contributions,
    match_brand_to_corporation,
)


# ---------------------------------------------------------------------------
# _get_scored_candidates tests
# ---------------------------------------------------------------------------


class TestGetScoredCandidates:
    def test_returns_all_candidates_with_score_key(self):
        """All wikidata candidates returned with float 'score' key.

        Bug caught: _get_scored_candidates silently drops candidates.
        """
        wd = [
            {"name": "Apple Inc.", "qid": "Q312"},
            {"name": "Apple Records", "qid": "Q99"},
            {"name": "Apple Corps", "qid": "Q100"},
        ]
        results = _get_scored_candidates("Apple", wd, [])
        assert len(results) == 3
        for r in results:
            assert "score" in r
            assert isinstance(r["score"], float)

    def test_sorted_descending_by_score(self):
        """Candidates returned highest-score first.

        Bug caught: wrong sort direction causes prompt to show worst match first.
        """
        wd = [
            {"name": "Bose-Einstein Corp", "qid": "Q999"},   # low similarity
            {"name": "Bose Corporation", "qid": "Q174959"},   # high similarity
            {"name": "Bose Audio GmbH", "qid": "Q888"},       # medium similarity
        ]
        results = _get_scored_candidates("Bose", wd, [])
        scores = [r["score"] for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_merges_wikidata_and_oc_candidates(self):
        """Both wikidata and OC candidates are included and scored.

        Bug caught: OC candidates silently excluded from prompt options.
        """
        wd = [{"name": "Bose Corporation", "qid": "Q174959"}]
        oc = [{"name": "Bose Corp", "jurisdiction_code": "us_ma", "company_number": "123"}]
        results = _get_scored_candidates("Bose", wd, oc)
        assert len(results) == 2
        sources = {r["source"] for r in results}
        assert sources == {"wikidata", "opencorporates"}

    def test_empty_inputs_returns_empty_list(self):
        """Both lists empty returns [].

        Bug caught: crash on empty input.
        """
        assert _get_scored_candidates("Bose", [], []) == []

    def test_candidate_missing_name_field_scores_zero(self):
        """Candidate with missing 'name' gets score 0.0, does not raise.

        Bug caught: KeyError/AttributeError on malformed Wikidata response.
        """
        wd = [{"qid": "Q999"}]  # no 'name' key
        results = _get_scored_candidates("Bose", wd, [])
        assert len(results) == 1
        assert results[0]["score"] == 0.0

    def test_does_not_mutate_input_dicts(self):
        """Input candidate dicts are not mutated — copies are returned.

        Bug caught: callers reusing candidate dicts see unexpected 'score' key.
        """
        wd = [{"name": "Apple Inc.", "qid": "Q312"}]
        original = dict(wd[0])
        _get_scored_candidates("Apple", wd, [])
        assert wd[0] == original, "Input dict was mutated"


class TestGetScoredCandidatesAliasScoring:
    def test_uses_alias_score_when_higher_than_label_score(self):
        """When alias matches better than label, alias score is used.

        Bug caught: AMD scores ~0.2 against 'Advanced Micro Devices' and never
        resolves. With alias='AMD', max(0.2, 1.0) = 1.0 — passes threshold.
        """
        wd = [{"name": "Advanced Micro Devices", "alias": "AMD", "qid": "Q336958"}]
        results = _get_scored_candidates("AMD", wd, [])
        assert len(results) == 1
        assert results[0]["score"] >= 0.7

    def test_uses_label_score_when_no_alias_key(self):
        """When no alias key present, label similarity is used (existing behaviour preserved).

        Bug caught: alias change accidentally breaks normal label-based scoring.
        """
        wd = [{"name": "Apple Inc.", "qid": "Q312"}]  # no 'alias' key
        results = _get_scored_candidates("Apple", wd, [])
        assert len(results) == 1
        assert results[0]["score"] >= 0.7

    def test_alias_none_does_not_raise(self):
        """alias=None in result dict must not crash similarity scoring.

        Bug caught: wd.get('alias', '') returns None when key present-with-None
        value; similarity(brand, None) raises TypeError.
        Fix: use wd.get('alias') or '' to treat None same as absent.
        """
        wd = [{"name": "Bose Corporation", "alias": None, "qid": "Q174959"}]
        results = _get_scored_candidates("Bose", wd, [])
        assert len(results) == 1
        # Must not raise; score should come from label path
        assert results[0]["score"] > 0.0


class TestMatchBrandToCorporationRegression:
    def test_returns_best_match_above_threshold(self):
        """Returns the highest-scoring candidate when score >= 0.7."""
        wd = [{"name": "Apple Inc.", "qid": "Q312"}]
        result = match_brand_to_corporation("Apple", wd, [])
        assert result is not None
        assert result["name"] == "Apple Inc."
        assert result["score"] >= 0.7

    def test_returns_none_when_all_below_threshold(self):
        """Returns None when no candidate meets the threshold."""
        wd = [{"name": "Totally Different Corp", "qid": "Q999"}]
        result = match_brand_to_corporation("Apple", wd, [])
        assert result is None

    def test_returns_none_for_empty_candidates(self):
        """Returns None when both candidate lists are empty."""
        assert match_brand_to_corporation("Apple", [], []) is None


# ---------------------------------------------------------------------------
# filter_corporate_pacs tests
# ---------------------------------------------------------------------------


def test_filter_corporate_pacs_includes_connected_org_name():
    """PAC with non-empty connected_org_name is included regardless of designation."""
    committees = [
        {"connected_org_name": "ACME Corp", "designation": "U", "interest_group_category": ""}
    ]
    result = filter_corporate_pacs(committees)
    assert len(result) == 1


def test_filter_corporate_pacs_includes_org_type_c():
    """PAC with interest_group_category='C' (Corporation) is included even with no connected_org."""
    committees = [{"connected_org_name": "", "designation": "U", "interest_group_category": "C"}]
    result = filter_corporate_pacs(committees)
    assert len(result) == 1


def test_filter_corporate_pacs_excludes_lobbyist_pac():
    """Lobbyist PAC (designation='B') with no corporate signals is excluded."""
    committees = [{"connected_org_name": "", "designation": "B", "interest_group_category": "L"}]
    result = filter_corporate_pacs(committees)
    assert result == []


def test_filter_corporate_pacs_excludes_leadership_pac():
    """Leadership PAC (designation='D') with no corporate signals is excluded."""
    committees = [{"connected_org_name": "", "designation": "D", "interest_group_category": ""}]
    result = filter_corporate_pacs(committees)
    assert result == []


def test_filter_corporate_pacs_excludes_whitespace_connected_org():
    """Whitespace-only connected_org_name is treated as empty and does not trigger inclusion."""
    committees = [{"connected_org_name": "   ", "designation": "U", "interest_group_category": ""}]
    result = filter_corporate_pacs(committees)
    assert result == []


# ---------------------------------------------------------------------------
# filter_supported_contributions tests
# ---------------------------------------------------------------------------


def test_filter_supported_contributions_keeps_24k():
    """Transaction type 24K (direct contribution) is kept; accepts generator input."""
    rows = ({"transaction_type": "24K", "transaction_amount": "1000"} for _ in range(1))
    result = list(filter_supported_contributions(rows))
    assert len(result) == 1
    assert result[0]["transaction_type"] == "24K"


def test_filter_supported_contributions_keeps_24z():
    """Transaction type 24Z (in-kind support) is kept."""
    rows = iter([{"transaction_type": "24Z", "transaction_amount": "500"}])
    result = list(filter_supported_contributions(rows))
    assert len(result) == 1
    assert result[0]["transaction_type"] == "24Z"


def test_filter_supported_contributions_drops_24a_24n():
    """Opposition expenditure types 24A and 24N are dropped; 24K passes through."""
    rows = iter(
        [
            {"transaction_type": "24A"},
            {"transaction_type": "24N"},
            {"transaction_type": "24K"},
        ]
    )
    result = list(filter_supported_contributions(rows))
    assert len(result) == 1
    assert result[0]["transaction_type"] == "24K"


def test_filter_supported_contributions_handles_whitespace_transaction_type():
    """Whitespace around transaction_type is stripped before comparison."""
    rows = iter(
        [
            {"transaction_type": " 24K "},
            {"transaction_type": " 24A "},
        ]
    )
    result = list(filter_supported_contributions(rows))
    assert len(result) == 1
    assert (
        result[0]["transaction_type"] == " 24K "
    )  # original value preserved, only stripped for comparison
