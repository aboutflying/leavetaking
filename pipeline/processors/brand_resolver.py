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
import sys
from collections.abc import Callable
from pathlib import Path

from pipeline.fetchers.opencorporates import search_companies
from pipeline.fetchers.wikidata import find_corporation
from pipeline.processors.entity_resolution import _get_scored_candidates, match_brand_to_corporation

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
    prompt_fn: Callable[[str, list[dict]], dict | None] | None = None,
) -> dict | None:
    """Resolve a single brand name to its best corporate match.

    Tries Wikidata EntitySearch first. Falls back to OpenCorporates only when
    Wikidata returns no confident match AND the per-run OC budget remains.
    When no automatic match is found but candidates exist, calls prompt_fn
    (if provided) to let the user pick a candidate interactively.

    Args:
        brand_name: Consumer-facing brand name (e.g. "Apple", "Procter & Gamble").
        oc_calls_used: Single-element list used as a mutable counter for OC calls
                       made so far in this pipeline run.
        max_oc_calls: Maximum OC calls allowed per run (default 20, well within
                      the 50 req/day free tier).
        prompt_fn: Optional callback invoked when no automatic match is found but
                   candidates exist. Receives (brand_name, scored_candidates) and
                   returns a chosen candidate dict or None to skip. Exceptions
                   from prompt_fn propagate to the caller. When None, headless
                   behavior is preserved (returns None below threshold).

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
    oc_results: list[dict] = []
    if oc_calls_used[0] < max_oc_calls:
        try:
            oc_results = search_companies(brand_name)
        except Exception:
            logger.warning("OpenCorporates lookup failed for '%s'", brand_name, exc_info=True)
    else:
        logger.debug(
            "OC quota exhausted (%d/%d) — skipping '%s'", oc_calls_used[0], max_oc_calls, brand_name
        )

    if oc_results:
        oc_calls_used[0] += 1
        match = match_brand_to_corporation(brand_name, [], oc_results) or None
        if match is not None:
            logger.info("OpenCorporates resolved '%s' -> '%s'", brand_name, match.get("name"))
            return match

    # --- Interactive fallback (when prompt_fn provided and candidates exist) ---
    if prompt_fn is not None:
        candidates = _get_scored_candidates(brand_name, wd_results, oc_results)
        if candidates:
            return prompt_fn(brand_name, candidates)

    logger.debug("No match found for '%s' (Wikidata + OC both exhausted)", brand_name)
    return None


def _stdin_prompt(brand_name: str, candidates: list[dict]) -> dict | None:
    """Prompt the user at the terminal to pick a corporate match.

    Displays each candidate with its name, similarity score, and source identifier
    so the user can make an informed choice. Returns the chosen candidate dict,
    or None if the user skips.

    Returns None immediately (without blocking) when stdin is not a TTY, so that
    cron jobs and CI pipelines are unaffected.
    """
    if not sys.stdin.isatty():
        logger.warning(
            "stdin is not a TTY — skipping interactive prompt for '%s'", brand_name
        )
        return None

    print(f"\nBrand: {brand_name}")
    print("No confident match found. Candidates:")
    for i, c in enumerate(candidates, 1):
        name = c.get("name") or ""
        name_col = (name[:37] + "...") if len(name) > 40 else name
        score = c.get("score") or 0.0
        source = c.get("source", "?")
        if source == "wikidata":
            source_id = c.get("qid") or "?"
        else:
            jurisdiction = c.get("jurisdiction") or "?"
            company_number = c.get("company_number") or "?"
            source_id = f"{jurisdiction}/{company_number}"
        print(f"  {i}. {name_col:<40} score={score:.2f}  [{source:<15} {source_id}]")
    print("  s. Skip (record as unresolved)")
    print()

    while True:
        try:
            raw = input(f"Choice [1-{len(candidates)} / s]: ").strip().lower()
        except EOFError:
            logger.warning("EOF on stdin — treating as skip for '%s'", brand_name)
            return None
        if raw == "s":
            return None
        if raw.isdigit() and 1 <= int(raw) <= len(candidates):
            return candidates[int(raw) - 1]
        print(f"  Invalid choice, enter 1-{len(candidates)} or s.")


def resolve_all_brands(
    brand_names: list[str],
    cache_path: Path,
    max_oc_calls: int = 20,
    prompt_fn: Callable[[str, list[dict]], dict | None] | None = _stdin_prompt,
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
        len(brand_names),
        len(brand_names) - len(unresolved),
        len(unresolved),
    )

    for brand_name in unresolved:
        result = resolve_brand(brand_name, oc_calls_used, max_oc_calls, prompt_fn)
        cache[brand_name] = result  # None for no-match — prevents re-query on re-run
        _save_cache(cache, cache_path)  # incremental: write after every brand

    resolved = {k: v for k, v in cache.items() if v is not None}
    unmatched = [k for k in cache if cache[k] is None]
    if unmatched:
        logger.warning(
            "Could not resolve %d brand(s): %s",
            len(unmatched),
            ", ".join(sorted(unmatched)),
        )
    logger.info("Brand resolution complete: %d/%d resolved", len(resolved), len(brand_names))
    return resolved
