"""One-shot backfill: populate artist `aliases` from TMDB's `also_known_as`.

Reads tracked artists from `general.artists`, searches TMDB by name, fetches
each person's `also_known_as` list, and merges results into:
  - aliases (string[])
  - normalized_aliases (string[])
  - tmdb_person_id (int) -- cached so re-runs skip the search step

Idempotent via `$addToSet`. Re-runs over the same artists are no-ops unless
TMDB has added new aliases since last run.

Dry-run by default. Pass --apply to commit changes.

Usage:
  python3 scripts/backfill_aliases_from_tmdb.py \
      --uri "$ARTISTS_MONGODB_URI" \
      --db general
  python3 scripts/backfill_aliases_from_tmdb.py --apply ...

Skips artists that already have `tmdb_person_id` set unless --force is passed.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from requests.exceptions import RequestException

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import MongoConfig  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("backfill_aliases")

TMDB_BASE_URL = "https://api.themoviedb.org/3"
ARTISTS_COLLECTION = MongoConfig().artists_collection
_NORMALIZE_PATTERN = re.compile(r"\W+", re.UNICODE)


def _normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "").casefold()
    return _NORMALIZE_PATTERN.sub("", normalized)


def _search_query(artist: dict) -> str | None:
    return artist.get("english_name") or artist.get("korean_name") or None


_MAX_CANDIDATES_TO_VERIFY = 5


def _fetch_aliases(
    session: requests.Session,
    api_key: str,
    artist: dict,
    timeout: float,
) -> dict | None:
    """Return {_id, aliases, person_id, candidate_count} for one artist, or None.

    Verification: only accept a TMDB candidate whose `also_known_as` contains
    the artist's korean_name (NFKC-normalized). If zero or multiple candidates
    verify, skip — better to leave un-aliased than pollute with the wrong person.
    """
    query = _search_query(artist)
    if not query:
        logger.warning("artist %s has no english_name or korean_name -- skipping", artist["_id"])
        return None

    korean_name = artist.get("korean_name") or ""
    norm_korean = _normalize_name(korean_name)
    if not norm_korean:
        logger.info(
            "skipping '%s' (artist _id=%s): no korean_name to verify candidates",
            query, artist["_id"],
        )
        return None

    try:
        search = session.get(
            f"{TMDB_BASE_URL}/search/person",
            params={"api_key": api_key, "query": query, "include_adult": "false"},
            timeout=timeout,
        )
        search.raise_for_status()
        results = search.json().get("results", [])
        if not results:
            logger.info("no TMDB hit for '%s' (artist _id=%s)", query, artist["_id"])
            return None

        verified: list[tuple[int, list[str]]] = []
        for candidate in results[:_MAX_CANDIDATES_TO_VERIFY]:
            person_id = candidate.get("id")
            if not person_id:
                continue
            details = session.get(
                f"{TMDB_BASE_URL}/person/{person_id}",
                params={"api_key": api_key},
                timeout=timeout,
            )
            details.raise_for_status()
            aka = details.json().get("also_known_as", []) or []
            if any(_normalize_name(a) == norm_korean for a in aka):
                verified.append((person_id, aka))
    except RequestException as exc:
        logger.warning("TMDB error for '%s' (artist _id=%s): %s", query, artist["_id"], exc)
        return None

    if not verified:
        logger.info(
            "no candidate verified for '%s' (artist _id=%s, korean_name='%s', %d candidates checked)",
            query, artist["_id"], korean_name, min(len(results), _MAX_CANDIDATES_TO_VERIFY),
        )
        return None
    if len(verified) > 1:
        logger.info(
            "multiple verified candidates for '%s' (artist _id=%s) -- skipping to avoid ambiguity: person_ids=%s",
            query, artist["_id"], [v[0] for v in verified],
        )
        return None

    person_id, aka = verified[0]
    return {
        "_id": artist["_id"],
        "aliases": [a for a in aka if isinstance(a, str) and a.strip()],
        "person_id": person_id,
        "candidate_count": len(results),
        "query": query,
    }


def _build_op(result: dict, blocked_normalized: frozenset[str]) -> UpdateOne:
    """Build the upsert. Filters aliases whose normalized form matches another
    artist's primary name (catches group names like 'Red Velvet' / '소녀시대').
    """
    aliases = result["aliases"]
    pairs = [(a, _normalize_name(a)) for a in aliases]
    kept = [(a, n) for a, n in pairs if n and n not in blocked_normalized]
    dropped = [a for a, n in pairs if n and n in blocked_normalized]
    if dropped:
        logger.info(
            "filtered group/shared names from artist _id=%s: %s",
            result["_id"], dropped,
        )
    update: dict = {"$set": {"tmdb_person_id": result["person_id"]}}
    if kept:
        update["$addToSet"] = {
            "aliases": {"$each": [a for a, _ in kept]},
            "normalized_aliases": {"$each": [n for _, n in kept]},
        }
    return UpdateOne({"_id": result["_id"]}, update)


def _load_blocked_names(coll, self_ids: set) -> frozenset[str]:
    """Set of normalized english_name + korean_name + existing aliases across the
    artists collection. Aliases matching this set get dropped -- they belong to a
    different artist doc (e.g. group names like 'Red Velvet' / '少女時代')."""
    names: set[str] = set()
    projection = {"english_name": 1, "korean_name": 1, "aliases": 1, "normalized_aliases": 1}
    for doc in coll.find({}, projection):
        for field in ("english_name", "korean_name"):
            value = doc.get(field)
            if isinstance(value, str):
                normalized = _normalize_name(value)
                if normalized:
                    names.add(normalized)
        for alias in doc.get("aliases") or ():
            if isinstance(alias, str):
                normalized = _normalize_name(alias)
                if normalized:
                    names.add(normalized)
        for normalized in doc.get("normalized_aliases") or ():
            if isinstance(normalized, str) and normalized:
                names.add(normalized)
    return frozenset(names)


def backfill(
    db,
    api_key: str,
    apply: bool,
    force: bool,
    workers: int,
    timeout: float,
    limit: int | None,
) -> dict[str, int]:
    coll = db[ARTISTS_COLLECTION]
    # Only process artists tagged as Actor (TMDB person search is meaningless
    # for musicians/groups -- they're not in TMDB and produce false matches).
    query: dict = {"type": "Actor"}
    if not force:
        query["tmdb_person_id"] = {"$exists": False}
    projection = {"_id": 1, "english_name": 1, "korean_name": 1, "aliases": 1, "type": 1}
    cursor = coll.find(query, projection)
    if limit:
        cursor = cursor.limit(limit)
    artists = list(cursor)
    logger.info("loaded %d artists to process (force=%s)", len(artists), force)

    if not artists:
        return {"processed": 0, "matched": 0, "ambiguous": 0, "missing": 0, "updated": 0}

    blocked_normalized = _load_blocked_names(coll, {a["_id"] for a in artists})
    logger.info("loaded %d blocked names (group/shared) for alias filtering", len(blocked_normalized))

    session = requests.Session()
    matched: list[dict] = []
    ambiguous = 0
    missing = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_fetch_aliases, session, api_key, a, timeout) for a in artists]
        for fut in as_completed(futures):
            result = fut.result()
            if result is None:
                missing += 1
                continue
            if result["candidate_count"] > 5:
                ambiguous += 1
                logger.info(
                    "ambiguous: '%s' returned %d candidates (artist _id=%s, picked person_id=%s)",
                    result["query"], result["candidate_count"], result["_id"], result["person_id"],
                )
            matched.append(result)

    operations = [_build_op(r, blocked_normalized) for r in matched]
    updated = 0
    if apply and operations:
        try:
            res = coll.bulk_write(operations, ordered=False)
            updated = res.modified_count
        except PyMongoError as exc:
            logger.error("bulk_write failed: %s", exc)
            raise
    elif operations:
        logger.info("DRY-RUN: would issue %d bulk updates (pass --apply to commit)", len(operations))

    return {
        "processed": len(artists),
        "matched": len(matched),
        "ambiguous": ambiguous,
        "missing": missing,
        "updated": updated,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--uri", default=os.environ.get("ARTISTS_MONGODB_URI") or os.environ.get("MONGODB_URI"))
    parser.add_argument("--db", default=os.environ.get("ARTISTS_SOURCE_DATABASE", "general"))
    parser.add_argument("--api-key", default=os.environ.get("TMDB_API_KEY"))
    parser.add_argument("--apply", action="store_true", help="commit changes (default: dry-run)")
    parser.add_argument("--force", action="store_true", help="re-process artists that already have tmdb_person_id")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--limit", type=int, default=None, help="cap number of artists processed (for testing)")
    args = parser.parse_args()

    if not args.uri:
        logger.error("no Mongo URI provided (set ARTISTS_MONGODB_URI or MONGODB_URI, or pass --uri)")
        return 2
    if not args.api_key:
        logger.error("no TMDB API key provided (set TMDB_API_KEY or pass --api-key)")
        return 2

    client = MongoClient(args.uri)
    try:
        db = client[args.db]
        stats = backfill(
            db=db,
            api_key=args.api_key,
            apply=args.apply,
            force=args.force,
            workers=args.workers,
            timeout=args.timeout,
            limit=args.limit,
        )
    finally:
        client.close()

    logger.info(
        "done: processed=%d matched=%d ambiguous=%d missing=%d updated=%d (apply=%s)",
        stats["processed"], stats["matched"], stats["ambiguous"],
        stats["missing"], stats["updated"], args.apply,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
