"""Fetch corporate ownership data from Wikidata SPARQL endpoint."""

from __future__ import annotations

import logging
import time

import requests

from pipeline.config import settings

logger = logging.getLogger(__name__)

HEADERS = {"Accept": "application/sparql-results+json", "User-Agent": "PoliticalPurchaser/0.1"}


def query_sparql(sparql: str) -> list[dict]:
    """Execute a SPARQL query against Wikidata and return bindings."""
    resp = requests.get(
        settings.wikidata_sparql_endpoint,
        params={"query": sparql, "format": "json"},
        headers=HEADERS,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", {}).get("bindings", [])


def find_corporation(brand_name: str) -> list[dict]:
    """Find the corporate owner of a brand via Wikidata.

    Returns a list of matches with entity URI, label, and parent org if available.
    """
    sparql = """
    SELECT ?item ?itemLabel ?parent ?parentLabel ?ticker WHERE {
      ?item rdfs:label "%s"@en .
      OPTIONAL { ?item wdt:P749 ?parent . }
      OPTIONAL { ?item wdt:P414 ?exchange . ?item wdt:P249 ?ticker . }
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    LIMIT 10
    """ % brand_name.replace('"', '\\"')

    results = query_sparql(sparql)
    return [_extract_binding(r) for r in results]


def get_subsidiaries(corporation_qid: str) -> list[dict]:
    """Get all subsidiaries of a corporation by Wikidata QID.

    Args:
        corporation_qid: Wikidata entity ID (e.g. 'Q95')
    """
    sparql = """
    SELECT ?subsidiary ?subsidiaryLabel WHERE {
      ?subsidiary wdt:P749 wd:%s .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
    }
    """ % corporation_qid

    results = query_sparql(sparql)
    return [
        {
            "qid": _qid_from_uri(r["subsidiary"]["value"]),
            "name": r.get("subsidiaryLabel", {}).get("value", ""),
        }
        for r in results
    ]


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
        chain.append({
            "child_qid": _qid_from_uri(r["child"]["value"]),
            "child_name": r.get("childLabel", {}).get("value", ""),
            "parent_qid": _qid_from_uri(r["parent"]["value"]),
            "parent_name": r.get("parentLabel", {}).get("value", ""),
        })
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


def _extract_binding(binding: dict) -> dict:
    """Extract useful fields from a SPARQL result binding."""
    result = {
        "qid": _qid_from_uri(binding.get("item", {}).get("value", "")),
        "name": binding.get("itemLabel", {}).get("value", ""),
    }
    if "parent" in binding:
        result["parent_qid"] = _qid_from_uri(binding["parent"]["value"])
        result["parent_name"] = binding.get("parentLabel", {}).get("value", "")
    if "ticker" in binding:
        result["ticker"] = binding["ticker"]["value"]
    return result


def _qid_from_uri(uri: str) -> str:
    """Extract QID from a Wikidata entity URI."""
    return uri.rsplit("/", 1)[-1] if "/" in uri else uri
