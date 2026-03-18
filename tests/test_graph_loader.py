"""Tests for pipeline/loaders/graph_loader.py."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from pipeline.loaders.graph_loader import (
    load_candidate_committee_linkage,
    load_committee_contributions,
)


def _make_session() -> MagicMock:
    session = MagicMock()
    session.run = MagicMock()
    return session


def _linkage_row(**kwargs) -> dict:
    defaults = {
        "cand_id": "P00000001",
        "cand_election_yr": "2026",
        "fec_election_yr": "2026",
        "cmte_id": "C00000001",
        "cmte_tp": "P",
        "cmte_dsgn": "P",
        "linkage_id": "123456789012",
    }
    return {**defaults, **kwargs}


def _contribution_row(**kwargs) -> dict:
    defaults = {
        "committee_id": "C00000001",
        "candidate_id": "P00000001",
        "transaction_id": "TXN123",
        "transaction_amount": "5000.00",
        "transaction_date": "01012026",
        "cycle": 2026,
    }
    return {**defaults, **kwargs}


# ---------------------------------------------------------------------------
# load_candidate_committee_linkage tests
# ---------------------------------------------------------------------------

def test_load_candidate_committee_linkage_runs_merge_query():
    """Function calls session.run with a query containing AUTHORIZED_COMMITTEE."""
    session = _make_session()
    rows = iter([_linkage_row()])

    load_candidate_committee_linkage(session, rows)

    assert session.run.called
    # Extract the query from the first call (first positional arg)
    query_called = session.run.call_args_list[0][0][0]
    assert "AUTHORIZED_COMMITTEE" in query_called


def test_load_candidate_committee_linkage_logs_missing_candidate_warning(caplog):
    """Logs a WARNING for any cand_id not present in known_cand_ids."""
    session = _make_session()
    rows = iter([_linkage_row(cand_id="P00000001")])
    known_cand_ids = {"P99999999"}  # P00000001 is NOT in this set

    with caplog.at_level(logging.WARNING, logger="pipeline.loaders.graph_loader"):
        load_candidate_committee_linkage(session, rows, known_cand_ids=known_cand_ids)

    warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("P00000001" in msg for msg in warning_messages), (
        f"Expected warning about P00000001, got: {warning_messages}"
    )


# ---------------------------------------------------------------------------
# load_committee_contributions tests
# ---------------------------------------------------------------------------

def test_load_committee_contributions_uses_merge_on_tran_id():
    """Cypher uses MERGE keyed on tran_id (not CREATE) for amendment dedup."""
    session = _make_session()

    load_committee_contributions(session, [_contribution_row()])

    query_called = session.run.call_args_list[0][0][0]
    assert "MERGE" in query_called, "Expected MERGE, got CREATE"
    assert "tran_id" in query_called or "transaction_id" in query_called, (
        "Expected tran_id/transaction_id in MERGE key"
    )


def test_load_committee_contributions_sets_amount_on_merge():
    """Cypher includes SET so amendment values overwrite the original record."""
    session = _make_session()

    load_committee_contributions(session, [_contribution_row()])

    query_called = session.run.call_args_list[0][0][0]
    assert "SET" in query_called, (
        "Expected SET clause so amendments overwrite — MERGE without SET leaves stale values"
    )
