"""Fetch corporate data from OpenCorporates API."""

from __future__ import annotations

import logging

import requests

from pipeline.config import settings

logger = logging.getLogger(__name__)

OC_BASE = "https://api.opencorporates.com/v0.4"


def _params() -> dict:
    """Build base params with API token if available."""
    if settings.opencorporates_api_token:
        return {"api_token": settings.opencorporates_api_token}
    return {}


def search_companies(query: str, jurisdiction: str = "") -> list[dict]:
    """Search for companies by name.

    Args:
        query: Company name to search for.
        jurisdiction: Optional jurisdiction code (e.g. 'us_de' for Delaware).

    Returns:
        List of company result dicts.
    """
    params = _params()
    params["q"] = query
    if jurisdiction:
        params["jurisdiction_code"] = jurisdiction

    resp = requests.get(f"{OC_BASE}/companies/search", params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    companies = data.get("results", {}).get("companies", [])
    return [c["company"] for c in companies]


def get_company(jurisdiction: str, company_number: str) -> dict | None:
    """Get a specific company by jurisdiction and number.

    Args:
        jurisdiction: Jurisdiction code (e.g. 'us_de').
        company_number: Company registration number.

    Returns:
        Company data dict or None.
    """
    params = _params()
    resp = requests.get(
        f"{OC_BASE}/companies/{jurisdiction}/{company_number}",
        params=params,
        timeout=30,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json().get("results", {}).get("company")


def get_corporate_grouping(name: str) -> dict | None:
    """Search for a corporate grouping (parent company relationships).

    Args:
        name: Corporate grouping name.

    Returns:
        Corporate grouping data or None.
    """
    params = _params()
    params["q"] = name
    resp = requests.get(f"{OC_BASE}/corporate_groupings/search", params=params, timeout=30)
    resp.raise_for_status()

    results = resp.json().get("results", {}).get("corporate_groupings", [])
    if results:
        return results[0].get("corporate_grouping")
    return None


def get_subsidiary_statements(company_jurisdiction: str, company_number: str) -> list[dict]:
    """Get subsidiary relationship statements for a company.

    Returns:
        List of subsidiary statement dicts with relationship details.
    """
    params = _params()
    resp = requests.get(
        f"{OC_BASE}/companies/{company_jurisdiction}/{company_number}/statements",
        params=params,
        timeout=30,
    )
    if resp.status_code == 404:
        return []
    resp.raise_for_status()

    statements = resp.json().get("results", {}).get("statements", [])
    return [
        s["statement"]
        for s in statements
        if s.get("statement", {}).get("predicate") in ("subsidiary_of", "has_subsidiary")
    ]
