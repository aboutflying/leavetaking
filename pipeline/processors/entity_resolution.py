"""Entity resolution: map brand names to corporate entities."""

from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


def normalize_company_name(name: str) -> str:
    """Normalize a company name for matching.

    Strips common suffixes (Inc, LLC, Corp, etc.), lowercases, and removes punctuation.
    """
    name = name.strip().lower()
    # Remove common corporate suffixes
    suffixes = [
        r"\binc\.?$", r"\bllc\.?$", r"\bcorp\.?$", r"\bcorporation$",
        r"\bco\.?$", r"\bcompany$", r"\bltd\.?$", r"\blimited$",
        r"\bplc$", r"\bgroup$", r"\bholdings?$", r"\benterprise[s]?$",
    ]
    for suffix in suffixes:
        name = re.sub(suffix, "", name).strip()
    # Remove punctuation except hyphens
    name = re.sub(r"[^\w\s-]", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def similarity(a: str, b: str) -> float:
    """Compute string similarity between two names (0-1)."""
    return SequenceMatcher(None, normalize_company_name(a), normalize_company_name(b)).ratio()


def match_brand_to_corporation(
    brand_name: str,
    wikidata_results: list[dict],
    oc_results: list[dict],
    threshold: float = 0.7,
) -> dict | None:
    """Match a brand name to a corporate entity using Wikidata and OpenCorporates results.

    Returns the best match above the threshold, or None.
    """
    candidates = []

    # Score Wikidata results
    for wd in wikidata_results:
        wd_name = wd.get("name", "")
        score = similarity(brand_name, wd_name)
        candidates.append({
            "name": wd_name,
            "source": "wikidata",
            "qid": wd.get("qid"),
            "parent_qid": wd.get("parent_qid"),
            "parent_name": wd.get("parent_name"),
            "ticker": wd.get("ticker"),
            "score": score,
        })

    # Score OpenCorporates results
    for oc in oc_results:
        oc_name = oc.get("name", "")
        score = similarity(brand_name, oc_name)
        candidates.append({
            "name": oc_name,
            "source": "opencorporates",
            "oc_id": oc.get("opencorporates_url", "").rsplit("/", 2)[-2:]
            if oc.get("opencorporates_url")
            else None,
            "jurisdiction": oc.get("jurisdiction_code"),
            "company_number": oc.get("company_number"),
            "score": score,
        })

    # Return best match above threshold
    candidates.sort(key=lambda c: c["score"], reverse=True)
    if candidates and candidates[0]["score"] >= threshold:
        best = candidates[0]
        logger.info(
            "Matched brand '%s' -> '%s' (source=%s, score=%.2f)",
            brand_name, best["name"], best["source"], best["score"],
        )
        return best

    logger.warning("No match for brand '%s' above threshold %.2f", brand_name, threshold)
    return None


def filter_corporate_pacs(committees: list[dict]) -> list[dict]:
    """Filter FEC committee records to corporate PACs.

    Corporate PACs have designation 'B' (lobbyist/registrant PAC) or 'D' (leadership PAC)
    or connected_org_name is non-empty.
    """
    return [
        c for c in committees
        if c.get("connected_org_name", "").strip()
        or c.get("designation") in ("B", "D")
    ]


def filter_executive_donations(
    contributions: list[dict],
    executive_titles: set[str] | None = None,
) -> list[dict]:
    """Filter individual contributions to those from senior executives.

    Uses occupation/employer fields to identify executives.
    """
    if executive_titles is None:
        executive_titles = {
            "ceo", "cfo", "coo", "cto", "president", "chairman", "chairwoman",
            "vice president", "vp", "chief executive", "chief financial",
            "chief operating", "chief technology", "director", "executive",
            "managing director", "partner", "founder", "owner",
        }

    results = []
    for contrib in contributions:
        occupation = (contrib.get("occupation") or "").lower()
        if any(title in occupation for title in executive_titles):
            results.append(contrib)
    return results
