"""Resolve RawRating candidate names to FEC candidate IDs using Neo4j index."""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator

from neo4j import Session

from pipeline.fetchers.scorecards import RawRating

logger = logging.getLogger(__name__)

_SUFFIXES = {"JR", "SR", "II", "III", "IV"}


def normalize_fec_name(fec_name: str) -> str:
    """Normalize FEC 'LASTNAME, FIRSTNAME [MIDDLE] [SUFFIX]' to 'firstname [middle] lastname'.

    Suffix tokens (JR, SR, II, III, IV) are stripped from the right of the
    first-name portion. If no comma present, returns fec_name.lower().strip().

    Examples::

        'MENENDEZ, ROBERT'    -> 'robert menendez'
        'KENNEDY, ROBERT F JR' -> 'robert f kennedy'
        'BYRD, ROBERT C SR'   -> 'robert c byrd'
        'SMITH'               -> 'smith'
    """
    if "," not in fec_name:
        return fec_name.lower().strip()
    lastname, rest = fec_name.split(",", 1)
    tokens = rest.strip().split()
    while tokens and tokens[-1].upper() in _SUFFIXES:
        tokens.pop()
    return " ".join(tokens + [lastname]).lower().strip()


def normalize_scorecard_name(name: str) -> str:
    """Lowercase and collapse whitespace in a scorecard candidate name."""
    return " ".join(name.lower().split())


def build_candidate_index(session: Session) -> dict[tuple[str, str], list[str]]:
    """Build in-memory lookup from (normalized_name, state) -> [fec_candidate_id].

    Queries all Candidate nodes already loaded by run_fec(). FEC names are
    normalized via normalize_fec_name (reverses LAST, FIRST format and strips
    suffixes JR/SR/II/III/IV). State None is coerced to empty string.

    Returns a plain dict. Callers detect ambiguity by checking len(list) > 1.
    """
    result: dict[tuple[str, str], list[str]] = defaultdict(list)
    records = session.run(
        "MATCH (c:Candidate) "
        "RETURN c.fec_candidate_id AS fec_id, c.name AS name, c.state AS state"
    )
    for record in records:
        name = record["name"] or ""
        state = record["state"] or ""
        fec_id = record["fec_id"]
        if not name or not fec_id:
            continue
        key = (normalize_fec_name(name), state)
        result[key].append(fec_id)
    return dict(result)


def resolve_candidates(
    raw_ratings: Iterator[RawRating],
    index: dict[tuple[str, str], list[str]],
) -> Iterator[dict]:
    """Resolve RawRatings to fec_candidate_id using the candidate index.

    For each rating:

    - Normalize scorecard name via normalize_scorecard_name (lowercase + collapse whitespace)
    - Uppercase state
    - Lookup (name, state) in index
    - 0 matches: log WARNING, skip
    - 2+ matches: log WARNING (ambiguous), skip
    - 1 match: yield resolved dict

    Note: suffix stripping is FEC-side only (done during build_candidate_index).
    Scorecard names are NOT suffix-stripped to avoid false-positive matches.
    """
    for rating in raw_ratings:
        key = (normalize_scorecard_name(rating.candidate_name), rating.state.upper())
        matches = index.get(key, [])
        if len(matches) == 0:
            logger.warning(
                "No candidate match for %r / %s — skipping",
                rating.candidate_name,
                rating.state,
            )
            continue
        if len(matches) > 1:
            logger.warning(
                "Ambiguous match for %r / %s (%d candidates) — skipping",
                rating.candidate_name,
                rating.state,
                len(matches),
            )
            continue
        yield {
            "org_name": rating.org_name,
            "year": rating.year,
            "fec_candidate_id": matches[0],
            "score": rating.score,
        }
