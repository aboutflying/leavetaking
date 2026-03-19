"""Resolve brand names to corporate entities with incremental caching.

Wikidata EntitySearch is the primary resolver. OpenCorporates is used as a
guarded fallback when Wikidata returns no confident match and the per-run OC
call budget has not been exhausted.

Cache format (brand_resolutions.json):
  {
    "Apple":      {"name": "Apple Inc.", "qid": "Q312", ...},  # resolved
    "SC Johnson": null                                           # tried, no match
  }

Null entries prevent re-querying on re-runs. Brands absent from the cache
have not been tried yet.

NOTE: Not safe for concurrent access. Intended for single-process weekly batch.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from pipeline.fetchers.opencorporates import search_companies
from pipeline.fetchers.wikidata import find_corporation
from pipeline.processors.entity_resolution import match_brand_to_corporation

logger = logging.getLogger(__name__)


def _load_cache(cache_path: Path) -> dict[str, dict | None]:
    """Load cached resolutions from disk.

    Returns {} if the file is missing, unreadable, or corrupt.
    """
    if not cache_path.exists():
        return {}
    try:
        return json.loads(cache_path.read_text())
    except (json.JSONDecodeError, OSError):
        logger.warning("Cache file %s is corrupt or unreadable — starting fresh", cache_path)
        return {}


def _save_cache(cache: dict, cache_path: Path) -> None:
    """Persist the resolution cache to disk."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, indent=2))


def resolve_brand(
    brand_name: str,
    oc_calls_used: list[int],
    max_oc_calls: int = 20,
) -> dict | None:
    """Resolve a single brand name to its best corporate match.

    Tries Wikidata EntitySearch first. Falls back to OpenCorporates only when
    Wikidata returns no confident match AND the per-run OC budget remains.

    Args:
        brand_name: Consumer-facing brand name (e.g. "Apple", "Procter & Gamble").
        oc_calls_used: Single-element list used as a mutable counter for OC calls
                       made so far in this pipeline run.
        max_oc_calls: Maximum OC calls allowed per run (default 20, well within
                      the 50 req/day free tier).

    Returns:
        Best-match dict (with at least 'name' and 'qid'/'source' keys), or None.
    """
    # --- Wikidata primary ---
    wd_results = find_corporation(brand_name)  # never raises; returns [] on error
    match = match_brand_to_corporation(brand_name, wd_results, []) or None
    if match is not None:
        logger.info("Wikidata resolved '%s' -> '%s'", brand_name, match.get("name"))
        return match

    # --- OpenCorporates fallback (guarded by quota) ---
    if oc_calls_used[0] >= max_oc_calls:
        logger.debug("OC quota exhausted (%d/%d) — skipping '%s'", oc_calls_used[0], max_oc_calls, brand_name)
        return None

    try:
        oc_results = search_companies(brand_name)
    except Exception:
        logger.warning("OpenCorporates lookup failed for '%s'", brand_name, exc_info=True)
        return None

    oc_calls_used[0] += 1
    match = match_brand_to_corporation(brand_name, [], oc_results) or None
    if match is not None:
        logger.info("OpenCorporates resolved '%s' -> '%s'", brand_name, match.get("name"))
    else:
        logger.debug("No match found for '%s' (Wikidata + OC both exhausted)", brand_name)
    return match


def resolve_all_brands(
    brand_names: list[str],
    cache_path: Path,
    max_oc_calls: int = 20,
) -> dict[str, dict]:
    """Resolve a list of brand names, writing cache after each one.

    Skips brands already present in the cache (both resolved and null entries).
    Writes the cache to disk after every brand so that a mid-run failure only
    loses the current brand, not all previous results.

    Args:
        brand_names: List of consumer-facing brand names to resolve.
        cache_path: Path to the JSON cache file (created if absent).
        max_oc_calls: Per-run OpenCorporates call budget (default 20).

    Returns:
        Dict mapping brand name -> resolution dict for successfully resolved brands only.
        Brands with no match are stored as null in the cache but excluded from the return value.
    """
    cache = _load_cache(cache_path)
    oc_calls_used = [0]

    unresolved = [b for b in brand_names if b not in cache]
    logger.info(
        "Brand resolution: %d brands total, %d already cached, %d to resolve",
        len(brand_names), len(brand_names) - len(unresolved), len(unresolved),
    )

    for brand_name in unresolved:
        result = resolve_brand(brand_name, oc_calls_used, max_oc_calls)
        cache[brand_name] = result  # None for no-match — prevents re-query on re-run
        _save_cache(cache, cache_path)  # incremental: write after every brand

    resolved = {k: v for k, v in cache.items() if v is not None}
    unmatched = [k for k in cache if cache[k] is None]
    if unmatched:
        logger.warning(
            "Could not resolve %d brand(s): %s",
            len(unmatched), ", ".join(sorted(unmatched)),
        )
    logger.info("Brand resolution complete: %d/%d resolved", len(resolved), len(brand_names))
    return resolved
