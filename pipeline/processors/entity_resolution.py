"""Entity resolution: map brand names to corporate entities."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


def normalize_company_name(name: str) -> str:
    """Normalize a company name for matching.

    Strips common suffixes (Inc, LLC, Corp, etc.), lowercases, and removes punctuation.
    """
    name = name.strip().lower()
    # Remove common corporate suffixes
    suffixes = [
        r"\binc\.?$",
        r"\bllc\.?$",
        r"\bcorp\.?$",
        r"\bcorporation$",
        r"\bco\.?$",
        r"\bcompany$",
        r"\bltd\.?$",
        r"\blimited$",
        r"\bplc$",
        r"\bgroup$",
        r"\bholdings?$",
        r"\benterprise[s]?$",
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


def _get_scored_candidates(
    brand_name: str,
    wikidata_results: list[dict],
    oc_results: list[dict],
) -> list[dict]:
    """Return all candidates from both sources scored and sorted by similarity descending.

    Each returned dict is a fresh copy annotated with a 'score' float (0-1).
    Input dicts are never mutated. Candidates with a missing or empty 'name'
    receive score 0.0 rather than raising.
    """
    scored: list[dict] = []

    for wd in wikidata_results:
        wd_name = wd.get("name", "")
        alias = wd.get("alias") or ""  # handles absent key and None value safely
        label_score = similarity(brand_name, wd_name) if wd_name else 0.0
        alias_score = similarity(brand_name, alias) if alias else 0.0
        score = max(label_score, alias_score)
        scored.append(
            {
                "name": wd_name,
                "source": "wikidata",
                "qid": wd.get("qid"),
                "parent_qid": wd.get("parent_qid"),
                "parent_name": wd.get("parent_name"),
                "ticker": wd.get("ticker"),
                "score": score,
            }
        )

    for oc in oc_results:
        oc_name = oc.get("name", "")
        score = similarity(brand_name, oc_name) if oc_name else 0.0
        scored.append(
            {
                "name": oc_name,
                "source": "opencorporates",
                "oc_id": oc.get("opencorporates_url", "").rsplit("/", 2)[-2:]
                if oc.get("opencorporates_url")
                else None,
                "jurisdiction": oc.get("jurisdiction_code"),
                "company_number": oc.get("company_number"),
                "score": score,
            }
        )

    scored.sort(key=lambda c: c["score"], reverse=True)
    return scored


def match_brand_to_corporation(
    brand_name: str,
    wikidata_results: list[dict],
    oc_results: list[dict],
    threshold: float = 0.7,
) -> dict | None:
    """Match a brand name to a corporate entity using Wikidata and OpenCorporates results.

    Returns the best match above the threshold, or None.
    """
    candidates = _get_scored_candidates(brand_name, wikidata_results, oc_results)

    if candidates and candidates[0]["score"] >= threshold:
        best = candidates[0]
        logger.info(
            "Matched brand '%s' -> '%s' (source=%s, score=%.2f)",
            brand_name,
            best["name"],
            best["source"],
            best["score"],
        )
        return best

    logger.warning("No match for brand '%s' above threshold %.2f", brand_name, threshold)
    return None


def _has_connected_org(committee: dict) -> bool:
    """Return True if the committee has a real connected_org_name.

    FEC data sometimes uses the literal string "NONE" instead of an empty field.
    """
    val = committee.get("connected_org_name", "").strip()
    return bool(val) and val.upper() != "NONE"


def filter_corporate_pacs(committees: list[dict]) -> list[dict]:
    """Filter FEC committee records to corporate PACs.

    Corporate PACs are identified by:
    - connected_org_name is non-empty and not "NONE" (PAC is connected to a
      sponsoring organization), OR
    - interest_group_category == 'C' (ORG_TP field: Corporation)

    Note: designation 'B' (lobbyist PAC) and 'D' (leadership PAC) are NOT reliable
    corporate PAC signals and are intentionally excluded from this filter.
    """
    return [
        c
        for c in committees
        if _has_connected_org(c) or c.get("interest_group_category", "").strip() == "C"
    ]


def resolve_pac_to_corporation(
    corporate_pacs: list[dict],
    corporation_names: list[str],
    threshold: float = 0.7,
) -> list[dict]:
    """Match corporate PACs to Corporation nodes by connected_org_name similarity.

    Args:
        corporate_pacs: Committee dicts with 'committee_id' and 'connected_org_name'.
        corporation_names: Corporation.name values already loaded into the graph.
        threshold: Minimum similarity score to accept a match (0–1).

    Returns:
        List of dicts with 'corporation_name' and 'committee_id' for each match.
    """
    edges = []
    for pac in corporate_pacs:
        org_name = pac.get("connected_org_name", "").strip()
        if not org_name or org_name.upper() == "NONE":
            continue

        best_name = None
        best_score = 0.0
        for corp_name in corporation_names:
            score = similarity(org_name, corp_name)
            if score > best_score:
                best_score = score
                best_name = corp_name

        if best_score >= threshold and best_name is not None:
            logger.debug(
                "Matched PAC '%s' (%s) -> '%s' (score=%.2f)",
                org_name,
                pac.get("committee_id"),
                best_name,
                best_score,
            )
            edges.append({"corporation_name": best_name, "committee_id": pac["committee_id"]})
        else:
            logger.debug(
                "No corporation match for PAC '%s' (%s) — best score=%.2f",
                org_name,
                pac.get("committee_id"),
                best_score,
            )

    return edges


def filter_supported_contributions(rows: Iterator[dict]) -> Iterator[dict]:
    """Yield only pas2 rows representing direct support to candidates.

    Filters to transaction types 24K (direct contribution) and 24Z (in-kind support).
    Drops opposition independent expenditures (24A, 24N) and all other types.
    """
    supported_types = {"24K", "24Z"}
    for row in rows:
        if (row.get("transaction_type") or "").strip() in supported_types:
            yield row


def filter_executive_donations(
    contributions: list[dict],
    executive_titles: set[str] | None = None,
) -> list[dict]:
    """Filter individual contributions to those from senior executives.

    Uses occupation/employer fields to identify executives.
    """
    if executive_titles is None:
        executive_titles = {
            "ceo",
            "cfo",
            "coo",
            "cto",
            "president",
            "chairman",
            "chairwoman",
            "vice president",
            "vp",
            "chief executive",
            "chief financial",
            "chief operating",
            "chief technology",
            "director",
            "executive",
            "managing director",
            "partner",
            "founder",
            "owner",
        }

    results = []
    for contrib in contributions:
        occupation = (contrib.get("occupation") or "").lower()
        if any(title in occupation for title in executive_titles):
            results.append(contrib)
    return results
