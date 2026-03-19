"""Tests for pipeline processors and entity resolution."""

from __future__ import annotations

from pipeline.processors.entity_resolution import (
    filter_corporate_pacs,
    filter_executive_donations,
    match_brand_to_corporation,
    normalize_company_name,
    resolve_pac_to_corporation,
    similarity,
)


class TestNormalizeCompanyName:
    def test_strips_inc(self):
        assert normalize_company_name("Apple Inc.") == "apple"
        assert normalize_company_name("Apple Inc") == "apple"

    def test_strips_llc(self):
        assert normalize_company_name("SomeCompany LLC") == "somecompany"

    def test_strips_corporation(self):
        assert normalize_company_name("Microsoft Corporation") == "microsoft"

    def test_strips_multiple_suffixes(self):
        assert normalize_company_name("Acme Holdings Corp.") == "acme"

    def test_preserves_hyphens(self):
        assert normalize_company_name("Procter-Gamble Co.") == "procter-gamble"

    def test_collapses_whitespace(self):
        # "Company" is a corporate suffix and gets stripped
        assert normalize_company_name("  Some   Company  ") == "some"
        assert normalize_company_name("  Some   Brand  Name  ") == "some brand name"


class TestSimilarity:
    def test_identical(self):
        assert similarity("Apple", "Apple") == 1.0

    def test_case_insensitive(self):
        assert similarity("apple", "Apple") == 1.0

    def test_with_suffix(self):
        assert similarity("Apple", "Apple Inc.") == 1.0

    def test_different(self):
        assert similarity("Apple", "Samsung") < 0.5


class TestMatchBrandToCorporation:
    def test_exact_match_wikidata(self):
        wd = [{"name": "Apple Inc.", "qid": "Q312", "source": "wikidata"}]
        result = match_brand_to_corporation("Apple", wd, [])
        assert result is not None
        assert result["source"] == "wikidata"

    def test_no_match_below_threshold(self):
        wd = [{"name": "Completely Different Corp", "qid": "Q999"}]
        result = match_brand_to_corporation("Apple", wd, [])
        assert result is None

    def test_prefers_higher_similarity(self):
        wd = [
            {"name": "Apple Inc.", "qid": "Q312"},
            {"name": "Applebee's", "qid": "Q500"},
        ]
        result = match_brand_to_corporation("Apple", wd, [])
        assert result is not None
        assert "Q312" in str(result.get("qid"))


class TestFilterCorporatePacs:
    def test_filters_by_connected_org(self):
        committees = [
            {"committee_id": "C001", "connected_org_name": "Apple Inc", "designation": "U"},
            {"committee_id": "C002", "connected_org_name": "", "designation": "U"},
        ]
        result = filter_corporate_pacs(committees)
        assert len(result) == 1
        assert result[0]["committee_id"] == "C001"

    def test_filters_by_org_type_corporation(self):
        # interest_group_category='C' (ORG_TP=Corporation) triggers inclusion.
        # designation 'B' (lobbyist PAC) alone is NOT a corporate PAC signal.
        committees = [
            {
                "committee_id": "C001",
                "connected_org_name": "",
                "designation": "B",
                "interest_group_category": "C",
            },
            {
                "committee_id": "C002",
                "connected_org_name": "",
                "designation": "P",
                "interest_group_category": "",
            },
        ]
        result = filter_corporate_pacs(committees)
        assert len(result) == 1
        assert result[0]["committee_id"] == "C001"

    def test_excludes_none_string_connected_org(self):
        # FEC data sometimes has literal "NONE" instead of empty string.
        committees = [
            {"committee_id": "C001", "connected_org_name": "NONE", "interest_group_category": ""},
            {
                "committee_id": "C002",
                "connected_org_name": "Apple Inc",
                "interest_group_category": "",
            },
        ]
        result = filter_corporate_pacs(committees)
        assert len(result) == 1
        assert result[0]["committee_id"] == "C002"


class TestResolvePacToCorporation:
    def test_matches_by_name(self):
        pacs = [{"committee_id": "C001", "connected_org_name": "Apple Inc."}]
        result = resolve_pac_to_corporation(pacs, ["Apple"])
        assert result == [{"corporation_name": "Apple", "committee_id": "C001"}]

    def test_no_match_below_threshold(self):
        pacs = [{"committee_id": "C001", "connected_org_name": "XYZ CORPORATION"}]
        result = resolve_pac_to_corporation(pacs, ["Apple", "Microsoft"])
        assert result == []

    def test_skips_empty_connected_org(self):
        pacs = [{"committee_id": "C001", "connected_org_name": ""}]
        result = resolve_pac_to_corporation(pacs, ["Apple"])
        assert result == []

    def test_skips_none_string_connected_org(self):
        pacs = [{"committee_id": "C001", "connected_org_name": "NONE"}]
        result = resolve_pac_to_corporation(pacs, ["Apple"])
        assert result == []

    def test_picks_best_match(self):
        pacs = [{"committee_id": "C001", "connected_org_name": "Microsoft Corporation"}]
        result = resolve_pac_to_corporation(pacs, ["Apple", "Microsoft", "Micro Systems"])
        assert len(result) == 1
        assert result[0]["corporation_name"] == "Microsoft"

    def test_multiple_pacs(self):
        pacs = [
            {"committee_id": "C001", "connected_org_name": "Apple Inc."},
            {"committee_id": "C002", "connected_org_name": "Exxon Mobil Corporation"},
            {"committee_id": "C003", "connected_org_name": "NONE"},
        ]
        result = resolve_pac_to_corporation(pacs, ["Apple", "Exxon Mobil", "Microsoft"])
        assert len(result) == 2
        ids = {r["committee_id"] for r in result}
        assert ids == {"C001", "C002"}


class TestFilterExecutiveDonations:
    def test_filters_executives(self):
        contribs = [
            {"contributor_name": "John Doe", "occupation": "CEO", "employer": "ACME"},
            {"contributor_name": "Jane Doe", "occupation": "TEACHER", "employer": "School"},
            {"contributor_name": "Bob Smith", "occupation": "VICE PRESIDENT", "employer": "Corp"},
        ]
        result = filter_executive_donations(contribs)
        assert len(result) == 2
        names = {r["contributor_name"] for r in result}
        assert "John Doe" in names
        assert "Bob Smith" in names

    def test_empty_input(self):
        assert filter_executive_donations([]) == []
