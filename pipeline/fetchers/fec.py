"""Fetch FEC campaign finance data (bulk files and API)."""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from collections.abc import Iterator
from pathlib import Path

import requests

from pipeline.config import settings

logger = logging.getLogger(__name__)

FEC_BULK_BASE = "https://www.fec.gov/files/bulk-downloads"
OPENFEC_BASE = "https://api.open.fec.gov/v1"

# FEC bulk file column definitions
COMMITTEE_MASTER_COLS = [
    "committee_id", "committee_name", "treasurer_name", "street1", "street2",
    "city", "state", "zip", "designation", "type", "party", "filing_frequency",
    "interest_group_category", "connected_org_name", "candidate_id",
]

CANDIDATE_MASTER_COLS = [
    "candidate_id", "candidate_name", "party", "election_year", "office_state",
    "office", "office_district", "incumbent_challenger_status", "candidate_status",
    "principal_committee_id", "street1", "street2", "city", "state", "zip",
]

# Contributions from committees to candidates (pas2)
COMMITTEE_CONTRIB_COLS = [
    "committee_id", "amendment_indicator", "report_type", "primary_general",
    "image_number", "transaction_type", "entity_type", "contributor_name",
    "city", "state", "zip", "employer", "occupation", "transaction_date",
    "transaction_amount", "other_id", "candidate_id", "transaction_id",
    "file_number", "memo_code", "memo_text", "sub_id",
]

# Candidate-committee linkage (ccl26.zip extracts to ccl26.txt — glob 'ccl*.txt' matches)
CANDIDATE_COMMITTEE_LINKAGE_COLS = [
    "cand_id", "cand_election_yr", "fec_election_yr",
    "cmte_id", "cmte_tp", "cmte_dsgn", "linkage_id",
]


def download_bulk_file(file_type: str, cycle: int) -> Path:
    """Download an FEC bulk data ZIP file and extract it.

    Args:
        file_type: One of 'cm' (committee master), 'cn' (candidate master),
                   'pas2' (committee-to-candidate), 'ccl' (candidate-committee linkage).
        cycle: Election cycle year (e.g. 2024).

    Returns:
        Path to the extracted data file.
    """
    cycle_suffix = str(cycle)[2:]  # e.g. "26" for 2026
    url = f"{FEC_BULK_BASE}/{cycle}/{file_type}{cycle_suffix}.zip"

    output_dir = settings.fec_bulk_data_dir / str(cycle)
    output_dir.mkdir(parents=True, exist_ok=True)

    zip_path = output_dir / f"{file_type}{cycle_suffix}.zip"

    if not zip_path.exists():
        logger.info("Downloading FEC bulk file: %s", url)
        resp = requests.get(url, timeout=300)
        resp.raise_for_status()
        zip_path.write_bytes(resp.content)
    else:
        logger.info("Using cached FEC bulk file: %s", zip_path)

    # Extract
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        zf.extractall(output_dir)
        logger.info("Extracted %d files from %s", len(names), zip_path.name)

    # Return path to first extracted file (the .txt data file)
    txt_files = list(output_dir.glob(f"{file_type}*.txt"))
    if txt_files:
        return txt_files[0]

    # Some files extract as .csv or other extensions
    extracted = [output_dir / n for n in names]
    return extracted[0] if extracted else output_dir


def parse_committee_master(path: Path) -> Iterator[dict]:
    """Stream committee master file as dicts."""
    yield from _stream_pipe_delimited(path, COMMITTEE_MASTER_COLS)


def parse_candidate_master(path: Path) -> Iterator[dict]:
    """Stream candidate master file as dicts."""
    yield from _stream_pipe_delimited(path, CANDIDATE_MASTER_COLS)


def parse_committee_contributions(path: Path) -> Iterator[dict]:
    """Stream committee-to-candidate contributions (pas2) file as dicts."""
    yield from _stream_pipe_delimited(path, COMMITTEE_CONTRIB_COLS)


def parse_candidate_committee_linkage(path: Path) -> Iterator[dict]:
    """Stream candidate-committee linkage (ccl) file as dicts."""
    yield from _stream_pipe_delimited(path, CANDIDATE_COMMITTEE_LINKAGE_COLS)


def _stream_pipe_delimited(
    path: Path,
    columns: list[str],
    log_interval: int = 50_000,
) -> Iterator[dict]:
    """Stream a pipe-delimited FEC bulk file, yielding one dict per row.

    Logs progress every ``log_interval`` rows so long-running files (e.g. pas2)
    produce visible output instead of appearing to hang.
    """
    file_size = path.stat().st_size
    logger.info("Streaming FEC bulk file: %s (%s)", path.name, _fmt_bytes(file_size))
    # Open in binary mode so bf.tell() works alongside csv.reader's next() calls.
    # (text-mode tell() raises OSError once next() has been called on the iterator.)
    with open(path, "rb") as bf:
        f = io.TextIOWrapper(bf, encoding="latin-1")
        reader = csv.reader(f, delimiter="|")
        row_count = 0
        for row in reader:
            if len(row) >= len(columns):
                yield dict(zip(columns, row[: len(columns)]))
            row_count += 1
            if row_count % log_interval == 0:
                pct = bf.tell() / file_size * 100 if file_size else 0
                logger.info("  %s: %d rows streamed (%.0f%%)", path.name, row_count, pct)
        f.detach()  # prevent TextIOWrapper from closing bf on __exit__
    logger.info("  %s: complete — %d rows", path.name, row_count)


def _fmt_bytes(n: int) -> str:
    """Format a byte count as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}"
        n //= 1024
    return f"{n} TB"


def fetch_committee_by_name(name: str) -> list[dict]:
    """Search for a committee by name using the OpenFEC API."""
    resp = requests.get(
        f"{OPENFEC_BASE}/names/committees/",
        params={"q": name, "api_key": settings.fec_api_key},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])


def fetch_candidate(candidate_id: str) -> dict | None:
    """Fetch a candidate by FEC ID."""
    resp = requests.get(
        f"{OPENFEC_BASE}/candidate/{candidate_id}/",
        params={"api_key": settings.fec_api_key},
        timeout=30,
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    return results[0] if results else None
