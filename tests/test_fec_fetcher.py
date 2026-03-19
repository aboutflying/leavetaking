"""Tests for pipeline/fetchers/fec.py — generator streaming and ccl26 support."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch


import pipeline.fetchers.fec as fec_module
from pipeline.fetchers.fec import (
    CANDIDATE_COMMITTEE_LINKAGE_COLS,
    COMMITTEE_MASTER_COLS,
    download_bulk_file,
    parse_candidate_committee_linkage,
    parse_committee_master,
)


def _write_pipe_delimited(tmp_path: Path, filename: str, rows: list[list[str]]) -> Path:
    """Write a pipe-delimited file for testing."""
    path = tmp_path / filename
    path.write_text("\n".join("|".join(row) for row in rows), encoding="latin-1")
    return path


def _make_zip_bytes(inner_filename: str, content: str) -> bytes:
    """Create an in-memory ZIP file containing a single text file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(inner_filename, content)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Test 1: parse_committee_master yields dicts with correct keys
# ---------------------------------------------------------------------------


def test_stream_committee_master_yields_dicts(tmp_path):
    """Generator yields exactly one dict per row with expected keys."""
    rows = [
        ["C00000001", "PAC One"] + ["x"] * 13,
        ["C00000002", "PAC Two"] + ["x"] * 13,
        ["C00000003", "PAC Three"] + ["x"] * 13,
    ]
    path = _write_pipe_delimited(tmp_path, "cm26.txt", rows)

    results = list(parse_committee_master(path))

    assert len(results) == 3
    for d in results:
        assert "committee_id" in d
        assert "connected_org_name" in d


# ---------------------------------------------------------------------------
# Test 2: Short rows (fewer fields than column count) are skipped
# ---------------------------------------------------------------------------


def test_stream_skips_short_rows(tmp_path):
    """Rows with fewer fields than column count are silently skipped."""
    # Only 3 fields — far fewer than len(COMMITTEE_MASTER_COLS) = 15
    rows = [["C00000001", "PAC One", "Treasurer"]]
    path = _write_pipe_delimited(tmp_path, "cm26.txt", rows)

    results = list(parse_committee_master(path))

    assert results == []


# ---------------------------------------------------------------------------
# Test 3: parse_candidate_committee_linkage yields dicts with all 7 keys
# ---------------------------------------------------------------------------


def test_stream_candidate_committee_linkage_yields_dicts(tmp_path):
    """parse_candidate_committee_linkage yields dicts with all 7 ccl26 columns."""
    rows = [
        ["P00000001", "2026", "2026", "C00000001", "P", "P", "123456789012"],
        ["P00000002", "2026", "2026", "C00000002", "S", "P", "234567890123"],
    ]
    path = _write_pipe_delimited(tmp_path, "ccl26.txt", rows)

    results = list(parse_candidate_committee_linkage(path))

    assert len(results) == 2
    expected_keys = {
        "cand_id",
        "cand_election_yr",
        "fec_election_yr",
        "cmte_id",
        "cmte_tp",
        "cmte_dsgn",
        "linkage_id",
    }
    for d in results:
        assert set(d.keys()) == expected_keys


# ---------------------------------------------------------------------------
# Test 4: parse_individual_contributions has been removed
# ---------------------------------------------------------------------------


def test_parse_individual_contributions_removed():
    """parse_individual_contributions must not exist in the fec module."""
    assert not hasattr(fec_module, "parse_individual_contributions"), (
        "parse_individual_contributions was found in fec module — it should have been removed"
    )


# ---------------------------------------------------------------------------
# Test 5: Empty file yields nothing without raising an exception
# ---------------------------------------------------------------------------


def test_stream_empty_file_yields_nothing(tmp_path):
    """An empty FEC file produces no records and raises no exception."""
    path = tmp_path / "cm26.txt"
    path.write_bytes(b"")

    results = list(parse_committee_master(path))

    assert results == []


# ---------------------------------------------------------------------------
# Test 6: Rows with extra columns are truncated to column count
# ---------------------------------------------------------------------------


def test_stream_rows_with_extra_columns_truncated(tmp_path):
    """Rows with more fields than column definition are truncated — no extra keys leak."""
    # 20 fields, column definition has 15
    extra_row = ["val"] * 20
    path = _write_pipe_delimited(tmp_path, "cm26.txt", [extra_row])

    results = list(parse_committee_master(path))

    assert len(results) == 1
    assert len(results[0].keys()) == len(COMMITTEE_MASTER_COLS)


# ---------------------------------------------------------------------------
# Test 7: download_bulk_file builds correct URL for 'ccl' file type
# ---------------------------------------------------------------------------


def test_download_bulk_file_ccl_constructs_correct_url(tmp_path):
    """download_bulk_file('ccl', 2026) requests the correct FEC URL."""
    zip_bytes = _make_zip_bytes("ccl26.txt", "P00000001|2026|2026|C00000001|P|P|123456789012\n")

    mock_response = MagicMock()
    mock_response.content = zip_bytes
    mock_response.raise_for_status = MagicMock()

    with patch("pipeline.fetchers.fec.requests.get", return_value=mock_response) as mock_get:
        with patch("pipeline.fetchers.fec.settings") as mock_settings:
            mock_settings.fec_bulk_data_dir = tmp_path
            download_bulk_file("ccl", 2026)

    called_url = mock_get.call_args[0][0]
    assert called_url == "https://www.fec.gov/files/bulk-downloads/2026/ccl26.zip", (
        f"Expected ccl26.zip URL, got: {called_url}"
    )


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------


def test_candidate_committee_linkage_cols_has_seven_entries():
    """CANDIDATE_COMMITTEE_LINKAGE_COLS must have exactly 7 entries matching ccl26 schema."""
    assert len(CANDIDATE_COMMITTEE_LINKAGE_COLS) == 7
    assert CANDIDATE_COMMITTEE_LINKAGE_COLS == [
        "cand_id",
        "cand_election_yr",
        "fec_election_yr",
        "cmte_id",
        "cmte_tp",
        "cmte_dsgn",
        "linkage_id",
    ]
