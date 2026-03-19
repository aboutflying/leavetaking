"""Resolve RawRating candidate names to FEC candidate IDs using Neo4j index."""

from __future__ import annotations

import logging
import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterator

from neo4j import Session

from pipeline.fetchers.scorecards import RawRating

logger = logging.getLogger(__name__)

_SUFFIXES = {"JR", "SR", "II", "III", "IV", "MR", "MR.", "MS", "MS.", "DR", "DR.", "HON", "HON.", "THE"}


def normalize_fec_name(fec_name: str) -> str:
    """Normalize FEC 'LASTNAME, FIRSTNAME [MIDDLE] [SUFFIX]' to 'firstname lastname'.

    Middle initials/names (single letter, optionally followed by a period) and
    suffix tokens (JR, SR, II, III, IV) are stripped. Scorecard sources typically
    only carry first + last name, so middle tokens would prevent matching.
    If no comma present, returns fec_name.lower().strip().

    Examples::

        'MENENDEZ, ROBERT'       -> 'robert menendez'
        'STEFANIK, ELISE M.'     -> 'elise stefanik'
        'BONAMICI, SUZANNE MS.'  -> 'suzanne bonamici'
        'GRIFFITH, H MORGAN'     -> 'morgan griffith'
        'KENNEDY, ROBERT F JR'   -> 'robert kennedy'
        'SMITH'                  -> 'smith'
    """
    if "," not in fec_name:
        return fec_name.lower().strip()
    lastname, rest = fec_name.split(",", 1)
    tokens = rest.strip().split()
    while tokens and tokens[-1].upper() in _SUFFIXES:
        tokens.pop()
    if not tokens:
        return lastname.lower().strip()
    # If the first token is a bare initial (e.g. "H" in "H MORGAN"), skip it so
    # that "H MORGAN GRIFFITH" normalizes to "morgan griffith".
    # All remaining tokens (including middle names/initials) are preserved so
    # that "KENNEDY, ROBERT F JR" -> "robert f kennedy".
    if len(tokens) >= 2 and re.match(r"^[A-Za-z]\.?$", tokens[0]):
        tokens = tokens[1:]
    given = " ".join(tokens)
    return _strip_accents(f"{given} {lastname}").lower().strip()


def _strip_accents(s: str) -> str:
    """Decompose Unicode characters and drop combining marks (é -> e, á -> a)."""
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def normalize_scorecard_name(name: str) -> str:
    """Lowercase, strip accents, and collapse whitespace in a scorecard candidate name."""
    return " ".join(_strip_accents(name).lower().split())


def _fec_given_name_tokens(fec_name: str) -> list[str]:
    """Return all non-initial, non-suffix given name tokens from a FEC name.

    For 'CRUZ, RAFAEL EDWARD TED' returns ['rafael', 'edward', 'ted'].
    Used to build alternate index keys so any given name can match a scorecard.
    """
    if "," not in fec_name:
        return []
    _, rest = fec_name.split(",", 1)
    tokens = rest.strip().split()
    while tokens and tokens[-1].upper() in _SUFFIXES:
        tokens.pop()
    return [t.lower() for t in tokens if not re.match(r"^[A-Za-z]\.?$", t)]


def build_candidate_index(
    session: Session,
) -> dict[tuple[str, str], list[str]]:
    """Build name/state lookup for FEC candidates.

    Returns:
        index: (normalized_name, state) -> [fec_candidate_id]

    Each FEC candidate gets one index entry per non-initial given name token, so
    'CRUZ, RAFAEL EDWARD TED' produces keys for 'rafael cruz', 'edward cruz', and
    'ted cruz'. State None is coerced to empty string.
    """
    index: dict[tuple[str, str], list[str]] = defaultdict(list)
    records = session.run(
        "MATCH (c:Candidate) "
        "RETURN c.fec_candidate_id AS fec_id, c.name AS name, "
        "c.state AS state"
    )
    for record in records:
        name = record["name"] or ""
        state = record["state"] or ""
        fec_id = record["fec_id"]
        if not name or not fec_id:
            continue
        # Primary key using normalize_fec_name
        index[(normalize_fec_name(name), state)].append(fec_id)
        # Alternate keys for each additional given name token
        if "," in name:
            lastname = name.split(",", 1)[0].lower().strip()
            for token in _fec_given_name_tokens(name)[1:]:  # skip first — already covered
                alt_key = (f"{token} {lastname}", state)
                if fec_id not in index[alt_key]:
                    index[alt_key].append(fec_id)
    return dict(index)


def _first_names_compatible(a: str, b: str) -> bool:
    """Return True if either first name is a prefix of the other.

    Catches nickname/full-name pairs like 'tim'/'timothy', 'ben'/'benjamin'.
    Both arguments should already be lowercased.
    """
    return a.startswith(b) or b.startswith(a)


def _build_lastname_index(
    index: dict[tuple[str, str], list[str]],
) -> dict[tuple[str, str], dict[str, list[str]]]:
    """Derive a (lastname, state) -> {normalized_fullname -> [fec_ids]} fallback index.

    Used when a full first+last name lookup misses due to nickname differences
    (e.g. 'Ben' vs 'Benjamin'). Grouping by full name lets the caller distinguish
    same-person-multiple-candidacies (one full name, multiple IDs) from genuinely
    different people (multiple full names).

    Only indexes 2-word normalized names (firstname lastname). Compound last names
    like 'monica de la cruz' (3+ words) are excluded — they match correctly via
    primary lookup and would cause false collisions here.
    """
    result: dict[tuple[str, str], dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for (name, state), fec_ids in index.items():
        parts = name.split()
        if len(parts) != 2:
            continue  # compound lastname — skip to avoid false collisions
        lastname = parts[-1]
        for fec_id in fec_ids:
            result[(lastname, state)][name].append(fec_id)
    return {k: dict(v) for k, v in result.items()}


def _filter_by_party(fec_ids: list[str], party: str | None, id_to_party: dict[str, str]) -> list[str]:
    """Return fec_ids whose party matches party (single letter). If party is None
    or no candidates survive the filter, returns the original list unfiltered."""
    if not party:
        return fec_ids
    filtered = [fid for fid in fec_ids if id_to_party.get(fid, "")[:1] == party]
    return filtered if filtered else fec_ids


def resolve_candidates(
    raw_ratings: Iterator[RawRating],
    index: dict[tuple[str, str], list[str]],
    id_to_party: dict[str, str] | None = None,
) -> Iterator[dict]:
    """Resolve RawRatings to fec_candidate_id using the candidate index.

    Lookup strategy:
    1. (first + last, state) — exact normalized match
    2. (last, state) — fallback for nickname mismatches (e.g. 'Ben' vs 'Benjamin');
       only used when exactly one FEC candidate shares that last name + state

    Multiple matches on the primary lookup are treated as the same person across
    chambers/cycles (e.g. a House member who ran for Senate). A rating is emitted
    for each matching candidate ID so the person's full FEC history is covered.

    Multiple matches on the lastname fallback are treated as genuinely ambiguous
    (different people) and skipped with a WARNING.

    0 matches after both stages: log WARNING, skip.
    """
    id_to_party = id_to_party or {}
    lastname_index = _build_lastname_index(index)

    for rating in raw_ratings:
        state = rating.state.upper()
        normalized = normalize_scorecard_name(rating.candidate_name)
        key = (normalized, state)
        matches = index.get(key, [])

        if len(matches) == 0:
            # Fallback: last name + state
            scorecard_lastname = normalized.rsplit(" ", 1)[-1]
            by_name = lastname_index.get((scorecard_lastname, state), {})
            if len(by_name) == 0:
                logger.warning(
                    "No candidate match for %r / %s — skipping",
                    rating.candidate_name, state,
                )
                continue
            elif len(by_name) > 1:
                # Narrow by first-name prefix, then party if still ambiguous
                scorecard_first = normalized.rsplit(" ", 1)[0]
                narrowed = {
                    name: ids for name, ids in by_name.items()
                    if _first_names_compatible(scorecard_first, name.rsplit(" ", 1)[0])
                }
                if len(narrowed) > 1 and rating.party:
                    narrowed = {
                        name: _filter_by_party(ids, rating.party, id_to_party)
                        for name, ids in narrowed.items()
                        if _filter_by_party(ids, rating.party, id_to_party)
                    }
                if len(narrowed) == 0:
                    logger.warning(
                        "No candidate match for %r / %s — skipping",
                        rating.candidate_name, state,
                    )
                    continue
                elif len(narrowed) == 1:
                    matches = next(iter(narrowed.values()))
                    logger.debug(
                        "Resolved %r / %s via lastname+prefix fallback (%d candidac%s)",
                        rating.candidate_name, state,
                        len(matches), "y" if len(matches) == 1 else "ies",
                    )
                else:
                    logger.warning(
                        "Ambiguous lastname match for %r / %s (%d distinct names) — skipping",
                        rating.candidate_name, state, len(narrowed),
                    )
                    continue
            else:
                # One distinct full name — could be multiple IDs (cross-chamber)
                matches = next(iter(by_name.values()))
                logger.debug(
                    "Resolved %r / %s via lastname fallback (%d candidac%s)",
                    rating.candidate_name, state,
                    len(matches), "y" if len(matches) == 1 else "ies",
                )
        elif len(matches) > 1:
            logger.warning(
                "Ambiguous match for %r / %s (%d FEC IDs) — skipping",
                rating.candidate_name, state, len(matches),
            )
            continue

        for fec_id in matches:
            yield {
                "org_name": rating.org_name,
                "year": rating.year,
                "fec_candidate_id": fec_id,
                "score": rating.score,
            }
