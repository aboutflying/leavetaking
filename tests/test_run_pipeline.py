"""Tests for pipeline/run_pipeline.py — wiring of FEC data pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

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
        patch(f"{_PATCH_BASE}.parse_candidate_committee_linkage", return_value=iter([])) as mock_ccl,
        patch(f"{_PATCH_BASE}.load_candidates") as mock_load_cands,
        patch(f"{_PATCH_BASE}.load_committees") as mock_load_cmtes,
        patch(f"{_PATCH_BASE}.load_committee_contributions") as mock_load_contribs,
        patch(f"{_PATCH_BASE}.load_candidate_committee_linkage") as mock_load_linkage,
        patch(f"{_PATCH_BASE}.filter_corporate_pacs", return_value=[]) as mock_filter_pacs,
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
        }


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
        {"transaction_type": "24K", "transaction_amount": "1000", "committee_id": "C001",
         "candidate_id": "P001", "transaction_id": "TXN1", "transaction_date": "01012026"},
        {"transaction_type": "24A", "transaction_amount": "500", "committee_id": "C001",
         "candidate_id": "P001", "transaction_id": "TXN2", "transaction_date": "01012026"},
    ]
    fec_patches["parse_committee_contributions"].return_value = iter(rows_with_mixed_types)

    # Capture what load_committee_contributions actually receives
    loaded_rows = []
    fec_patches["load_committee_contributions"].side_effect = lambda session, rows: loaded_rows.extend(rows)

    run_fec(_make_session())

    transaction_types = [r["transaction_type"] for r in loaded_rows]
    assert "24A" not in transaction_types, (
        "Opposition expenditure 24A must be filtered out before loading"
    )
    assert "24K" in transaction_types, (
        "Direct support 24K must be passed through to loading"
    )


def test_run_fec_passes_known_cand_ids_to_linkage_loader(fec_patches):
    """run_fec must pass known_cand_ids (not None) to load_candidate_committee_linkage."""
    from pipeline.run_pipeline import run_fec

    # parse_candidate_master returns a candidate with a known ID
    fec_patches["parse_candidate_master"].return_value = iter([
        {"candidate_id": "P00000001", "candidate_name": "Test Candidate",
         "party": "DEM", "election_year": "2026", "office_state": "CA",
         "office": "H", "office_district": "01", "incumbent_challenger_status": "I",
         "candidate_status": "C", "principal_committee_id": "C001",
         "street1": "", "street2": "", "city": "", "state": "", "zip": ""},
    ])

    run_fec(_make_session())

    assert fec_patches["load_candidate_committee_linkage"].called, (
        "load_candidate_committee_linkage was not called"
    )
    call_kwargs = fec_patches["load_candidate_committee_linkage"].call_args[1]
    assert "known_cand_ids" in call_kwargs, (
        "known_cand_ids kwarg not passed — validation will be skipped"
    )
    assert call_kwargs["known_cand_ids"] is not None, (
        "known_cand_ids must be a set, not None"
    )
    assert isinstance(call_kwargs["known_cand_ids"], set), (
        f"known_cand_ids must be a set, got {type(call_kwargs['known_cand_ids'])}"
    )
