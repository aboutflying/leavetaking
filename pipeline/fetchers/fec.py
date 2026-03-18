"""Fetch FEC campaign finance data (bulk files and API)."""

from __future__ import annotations

import csv
import io
import logging
import zipfile
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

# Individual contributions (itcont)
INDIVIDUAL_CONTRIB_COLS = [
    "committee_id", "amendment_indicator", "report_type", "primary_general",
    "image_number", "transaction_type", "entity_type", "contributor_name",
    "city", "state", "zip", "employer", "occupation", "transaction_date",
    "transaction_amount", "other_id", "transaction_id", "file_number",
    "memo_code", "memo_text", "sub_id",
]


def download_bulk_file(file_type: str, cycle: int) -> Path:
    """Download an FEC bulk data ZIP file and extract it.

    Args:
        file_type: One of 'cm' (committee master), 'cn' (candidate master),
                   'pas2' (committee-to-candidate), 'indiv' (individual contributions).
        cycle: Election cycle year (e.g. 2024).

    Returns:
        Path to the extracted data file.
    """
    cycle_suffix = str(cycle)[2:]  # e.g. "24" for 2024
    url = f"{FEC_BULK_BASE}/{cycle}/{{file_type}}{cycle_suffix}.zip"
    url = url.format(file_type=file_type)

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


def parse_committee_master(path: Path) -> list[dict]:
    """Parse committee master file into list of dicts."""
    return _parse_pipe_delimited(path, COMMITTEE_MASTER_COLS)


def parse_candidate_master(path: Path) -> list[dict]:
    """Parse candidate master file into list of dicts."""
    return _parse_pipe_delimited(path, CANDIDATE_MASTER_COLS)


def parse_committee_contributions(path: Path) -> list[dict]:
    """Parse committee-to-candidate contributions (pas2) file."""
    return _parse_pipe_delimited(path, COMMITTEE_CONTRIB_COLS)


def parse_individual_contributions(path: Path) -> list[dict]:
    """Parse individual contributions (itcont) file."""
    return _parse_pipe_delimited(path, INDIVIDUAL_CONTRIB_COLS)


def _parse_pipe_delimited(path: Path, columns: list[str]) -> list[dict]:
    """Parse a pipe-delimited FEC bulk file."""
    records = []
    with open(path, encoding="latin-1") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) >= len(columns):
                records.append(dict(zip(columns, row[: len(columns)])))
    logger.info("Parsed %d records from %s", len(records), path.name)
    return records


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
