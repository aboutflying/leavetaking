"""Fetch corporate ownership data from Wikidata SPARQL endpoint."""

from __future__ import annotations

import logging
import time

import requests

from pipeline.config import settings

logger = logging.getLogger(__name__)

HEADERS = {"Accept": "application/sparql-results+json", "User-Agent": "PoliticalPurchaser/0.1"}

WIKIDATA_API = "https://www.wikidata.org/w/api.php"

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 2.0  # seconds; doubled each attempt


def _is_retryable(exc: requests.HTTPError) -> bool:
    """Return True if this HTTP error is transient and worth retrying."""
    if exc.response is not None:
        return exc.response.status_code in _RETRYABLE_STATUS
    # Fallback for cases where HTTPError is raised with just a status string
    msg = str(exc)
    return any(str(s) in msg for s in _RETRYABLE_STATUS)


def query_sparql(sparql: str) -> list[dict]:
    """Execute a SPARQL query against Wikidata and return bindings.

    Retries up to _MAX_RETRIES times with exponential backoff on transient
    5xx / 429 errors (e.g. 502 Bad Gateway from Wikidata infrastructure blips).
    Raises immediately on 4xx client errors — those are not transient.
    """
    last_exc: requests.HTTPError | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.get(
                settings.wikidata_sparql_endpoint,
                params={"query": sparql, "format": "json"},
                headers=HEADERS,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json().get("results", {}).get("bindings", [])
        except requests.HTTPError as exc:
            if not _is_retryable(exc):
                raise
            last_exc = exc
            delay = _RETRY_BASE_DELAY * (2**attempt)
            logger.warning(
                "Wikidata SPARQL transient error (attempt %d/%d): %s — retrying in %.1fs",
                attempt + 1,
                _MAX_RETRIES,
                exc,
                delay,
            )
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


def _search_entities(brand_name: str, limit: int = 5) -> list[str]:
    """Use the Wikidata wbsearchentities Action API to find entity QIDs by name.

    This is more reliable than wikibase:mwapi EntitySearch inside SPARQL, which
    returns HTTP 500 for popular brand names due to Wikidata query engine timeouts.
    """
    resp = requests.get(
        WIKIDATA_API,
        params={
            "action": "wbsearchentities",
            "search": brand_name,
            "type": "item",
            "language": "en",
            "limit": limit,
            "format": "json",
        },
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    results = []
    for item in resp.json().get("search", []):
        match_obj = item.get("match", {})
        alias = match_obj.get("text") if match_obj.get("type") == "alias" else None
        results.append({
            "qid": item["id"],
            "matched_alias": alias,
            "label": item.get("label", ""),
        })
    return results


def find_corporation(brand_name: str) -> list[dict]:
    """Find the corporate owner of a brand via Wikidata.

    Two-step approach:
    1. wbsearchentities Action API for fuzzy name search → {qid, matched_alias}
    2. SPARQL VALUES query filtered to business enterprises (Q4830453) → properties

    This avoids wikibase:mwapi EntitySearch inside SPARQL, which returns HTTP 500
    for popular brand names (e.g. 'Apple') due to Wikidata query engine timeouts.
    The matched_alias from step 1 is threaded into results so that acronym brands
    (AMD, ASUS) score correctly against their alias rather than the full legal name.
    """
    if not brand_name.strip():
        return []

    try:
        search_results = _search_entities(brand_name)
    except Exception:
        logger.exception("Wikidata entity search failed for brand '%s'", brand_name)
        return []

    if not search_results:
        return []

    # Build alias lookup and QID list from search results
    alias_by_qid = {d["qid"]: d["matched_alias"] for d in search_results if d["matched_alias"]}
    qid_list = [d["qid"] for d in search_results]

    values = " ".join(f"wd:{q}" for q in qid_list)
    sparql = """
    SELECT ?item ?itemLabel ?parent ?parentLabel ?ticker WHERE {
      VALUES ?item { %s }
      ?item wdt:P31/wdt:P279* wd:Q4830453 .
      OPTIONAL { ?item wdt:P749 ?parent . }
      OPTIONAL { ?item wdt:P249 ?ticker . }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    LIMIT 5
    """ % values

    try:
        bindings = query_sparql(sparql)
    except Exception:
        logger.exception("Wikidata SPARQL query failed for brand '%s'", brand_name)
        return []

    results = []
    for r in bindings:
        qid = _qid_from_uri(r.get("item", {}).get("value", ""))
        alias = alias_by_qid.get(qid)  # None if no alias for this QID
        results.append(_extract_binding(r, matched_alias=alias))
    return results


def get_subsidiaries(corporation_qid: str) -> list[dict]:
    """Get all subsidiaries of a corporation by Wikidata QID.

    Args:
        corporation_qid: Wikidata entity ID (e.g. 'Q95')
    """
    sparql = (
        """
    SELECT ?subsidiary ?subsidiaryLabel WHERE {
      ?subsidiary wdt:P749 wd:%s .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    """
        % corporation_qid
    )

    results = query_sparql(sparql)
    return [
        {
            "qid": _qid_from_uri(r["subsidiary"]["value"]),
            "name": r.get("subsidiaryLabel", {}).get("value", ""),
        }
        for r in results
    ]


_BRAND_DISCOVERY_LIMIT = 200


def discover_brands_for_corporation(corp_qid: str) -> list[dict]:
    """Find consumer brands owned by a corporation via Wikidata reverse P749.

    Returns entities that are children of corp_qid via P749 (parent org) but
    are NOT instances of Q4830453 (business enterprise) — i.e. consumer brand
    entities rather than subsidiary corporations (which are handled separately
    by discover_subsidiaries_for_corpus).

    Note: a returned entity may already exist as a Corporation node in Neo4j
    from subsidiary discovery. This is intentional — Neo4j nodes can have both
    Brand and Corporation labels for the same legal entity.

    Args:
        corp_qid: Wikidata entity ID (e.g. 'Q95'). Must start with 'Q'.

    Returns:
        List of {"name": str, "qid": str} dicts for brand entities.
    """
    if not corp_qid or not corp_qid.startswith("Q"):
        return []

    sparql = """
    SELECT ?item ?itemLabel WHERE {
      ?item wdt:P749 wd:%s .
      FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q4830453 }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    LIMIT %d
    """ % (
        corp_qid,
        _BRAND_DISCOVERY_LIMIT,
    )

    results = query_sparql(sparql)

    if len(results) >= _BRAND_DISCOVERY_LIMIT:
        logger.warning(
            "discover_brands_for_corporation(%s): result count hit LIMIT %d — results may be truncated",
            corp_qid,
            _BRAND_DISCOVERY_LIMIT,
        )

    brands = []
    for r in results:
        name = r.get("itemLabel", {}).get("value", "")
        if not name:
            continue
        brands.append(
            {
                "name": name,
                "qid": _qid_from_uri(r.get("item", {}).get("value", "")),
            }
        )
    return brands


def get_ownership_chain(entity_qid: str, max_depth: int = 10) -> list[dict]:
    """Traverse the ownership chain upward from an entity.

    Returns a list of (child, parent) pairs representing SUBSIDIARY_OF edges.
    """
    sparql = """
    SELECT ?child ?childLabel ?parent ?parentLabel WHERE {
      wd:%s wdt:P749* ?child .
      ?child wdt:P749 ?parent .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    LIMIT %d
    """ % (entity_qid, max_depth * 2)

    results = query_sparql(sparql)
    chain = []
    for r in results:
        chain.append(
            {
                "child_qid": _qid_from_uri(r["child"]["value"]),
                "child_name": r.get("childLabel", {}).get("value", ""),
                "parent_qid": _qid_from_uri(r["parent"]["value"]),
                "parent_name": r.get("parentLabel", {}).get("value", ""),
            }
        )
    return chain


def batch_resolve_brands(brand_names: list[str], delay: float = 1.0) -> dict[str, list[dict]]:
    """Resolve a list of brand names to corporate entities.

    Args:
        brand_names: List of brand name strings.
        delay: Seconds to wait between queries (respect rate limits).

    Returns:
        Dict mapping brand name to list of Wikidata matches.
    """
    results = {}
    for name in brand_names:
        try:
            matches = find_corporation(name)
            results[name] = matches
            logger.info("Resolved '%s': %d matches", name, len(matches))
        except Exception:
            logger.exception("Failed to resolve brand: %s", name)
            results[name] = []
        time.sleep(delay)
    return results


def _extract_binding(binding: dict, matched_alias: str | None = None) -> dict:
    """Extract useful fields from a SPARQL result binding.

    Args:
        binding: SPARQL result binding dict.
        matched_alias: Alias text from wbsearchentities when match.type==\"alias\",
                       e.g. \"AMD\" for the entity labeled \"Advanced Micro Devices\".
                       Only stored in result when truthy — prevents None leaking into
                       similarity scoring.
    """
    result = {
        "qid": _qid_from_uri(binding.get("item", {}).get("value", "")),
        "name": binding.get("itemLabel", {}).get("value", ""),
    }
    if "parent" in binding:
        result["parent_qid"] = _qid_from_uri(binding["parent"]["value"])
        result["parent_name"] = binding.get("parentLabel", {}).get("value", "")
    if "ticker" in binding:
        result["ticker"] = binding["ticker"]["value"]
    if matched_alias:
        result["alias"] = matched_alias
    return result


def _qid_from_uri(uri: str) -> str:
    """Extract QID from a Wikidata entity URI."""
    return uri.rsplit("/", 1)[-1] if "/" in uri else uri
