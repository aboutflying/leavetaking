"""Tests for pipeline/loaders/graph_loader.py."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock


from pipeline.loaders.graph_loader import (
    load_brands,
    load_candidate_committee_linkage,
    load_committee_contributions,
    load_corporations,
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


# ---------------------------------------------------------------------------
# load_corporations — qid storage tests
# ---------------------------------------------------------------------------


def test_load_corporations_stores_qid_when_present():
    """Cypher includes corp.qid so the Wikidata QID is written to the node.

    Bug caught: qid silently dropped from match dict — Corporation node never
    gets a qid property, blocking all downstream P355 discovery.
    """
    session = _make_session()

    load_corporations(session, [{"name": "Acme Corp", "qid": "Q123"}])

    query_called = session.run.call_args_list[0][0][0]
    assert "corp.qid" in query_called, (
        "Cypher must reference corp.qid — qid is being silently dropped from load_corporations"
    )
    batch_called = session.run.call_args_list[0][1]["batch"]
    assert batch_called[0].get("qid") == "Q123", (
        "qid value must be passed through in the batch parameter"
    )


def test_load_corporations_cypher_uses_null_safe_case_for_qid():
    """Cypher uses CASE WHEN c.qid IS NOT NULL guard, not unconditional SET.

    Bug caught: SET corp.qid = c.qid unconditionally overwrites existing qid
    with NULL when re-running pipeline with a dict that lacks qid.
    """
    session = _make_session()

    load_corporations(session, [{"name": "Acme Corp"}])

    query_called = session.run.call_args_list[0][0][0]
    assert "c.qid IS NOT NULL" in query_called, (
        "Cypher must guard qid assignment with IS NOT NULL to prevent writing NULL on re-runs"
    )


def test_load_corporations_skips_qid_when_none():
    """Cypher IS NOT NULL guard prevents writing explicit None qid to the node.

    Bug caught: qid=None in input dict writes NULL to the node, which breaks
    future 'WHERE c.qid IS NOT NULL' discovery queries.
    """
    session = _make_session()

    load_corporations(session, [{"name": "Acme Corp", "qid": None}])

    query_called = session.run.call_args_list[0][0][0]
    assert "c.qid IS NOT NULL" in query_called, (
        "CASE guard must be present so qid=None in input dict does not write NULL to graph"
    )


def test_load_corporations_preserves_existing_qid_when_new_dict_has_none():
    """Cypher CASE includes ELSE corp.qid so existing node qid is preserved.

    Bug caught: without ELSE corp.qid, a re-run with a dict that has no qid
    silently wipes the previously stored QID from the Corporation node.
    This breaks the P355 discovery pipeline on every subsequent run.
    """
    session = _make_session()

    load_corporations(session, [{"name": "Acme Corp"}])

    query_called = session.run.call_args_list[0][0][0]
    assert "ELSE corp.qid" in query_called, (
        "CASE must end with ELSE corp.qid to preserve existing qid when incoming dict has none"
    )


# ---------------------------------------------------------------------------
# load_brands — aliases CASE guard tests
# ---------------------------------------------------------------------------


def test_load_brands_writes_aliases_when_provided():
    """load_brands stores aliases when the incoming dict provides them.

    Bug caught: CASE guard must not block aliases from being written when
    caller explicitly passes a non-None list (e.g. ["alias1"]).
    """
    session = _make_session()

    load_brands(session, [{"name": "Nike", "amazon_slug": "nike", "aliases": ["Nike Inc."]}])

    query_called = session.run.call_args_list[0][0][0]
    assert "b.aliases IS NOT NULL" in query_called, (
        "CASE guard must check b.aliases IS NOT NULL so provided aliases are written"
    )


def test_load_brands_preserves_existing_aliases_when_none_passed():
    """load_brands does not overwrite existing aliases when incoming dict passes None.

    Bug caught: without CASE guard, `SET brand.aliases = b.aliases` overwrites
    existing aliases with None/[] on every re-run, destroying manually set alias data.
    The guard ensures: if b.aliases IS NULL, keep brand.aliases (existing value).
    """
    session = _make_session()

    load_brands(session, [{"name": "Nike", "amazon_slug": "nike", "aliases": None}])

    query_called = session.run.call_args_list[0][0][0]
    assert "ELSE brand.aliases" in query_called, (
        "CASE must end with ELSE brand.aliases to preserve existing aliases "
        "when incoming dict passes None"
    )
