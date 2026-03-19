"""Tests for pipeline/processors/entity_resolution.py."""

from __future__ import annotations

from pipeline.processors.entity_resolution import (
    filter_corporate_pacs,
    filter_supported_contributions,
)


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
