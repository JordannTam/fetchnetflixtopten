"""Rollback the polluting aliases written by the first --limit 10 backfill run.

For each (artist_id, wrong_tmdb_person_id) pair, re-fetch TMDB's
`also_known_as` for the wrong person and remove those exact strings (and their
normalized forms) from the artist's aliases / normalized_aliases. Then unset
tmdb_person_id.

Dry-run by default. Pass --apply to commit.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import unicodedata

import requests
from bson import ObjectId
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from requests.exceptions import RequestException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("rollback_aliases")

TMDB_BASE_URL = "https://api.themoviedb.org/3"
_NORMALIZE_PATTERN = re.compile(r"\W+", re.UNICODE)

def _load_polluted(coll) -> list[tuple[ObjectId, int]]:
    """All artists currently carrying a tmdb_person_id -- candidates for rollback."""
    return [
        (doc["_id"], doc["tmdb_person_id"])
        for doc in coll.find({"tmdb_person_id": {"$exists": True}}, {"_id": 1, "tmdb_person_id": 1})
    ]


def _normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "").casefold()
    return _NORMALIZE_PATTERN.sub("", normalized)


def _fetch_aka(session: requests.Session, api_key: str, person_id: int, timeout: float) -> list[str]:
    try:
        r = session.get(
            f"{TMDB_BASE_URL}/person/{person_id}",
            params={"api_key": api_key},
            timeout=timeout,
        )
        r.raise_for_status()
        return [a for a in (r.json().get("also_known_as") or []) if isinstance(a, str) and a.strip()]
    except RequestException as exc:
        logger.warning("TMDB fetch failed for person_id=%s: %s", person_id, exc)
        return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=os.environ.get("ARTISTS_MONGODB_URI") or os.environ.get("MONGODB_URI"))
    parser.add_argument("--db", default=os.environ.get("ARTISTS_SOURCE_DATABASE", "general"))
    parser.add_argument("--api-key", default=os.environ.get("TMDB_API_KEY"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    if not args.uri or not args.api_key:
        logger.error("missing --uri or --api-key (or env vars)")
        return 2

    client = MongoClient(args.uri)
    try:
        coll = client[args.db]["artists"]
        polluted = _load_polluted(coll)
        logger.info("found %d artists with tmdb_person_id to roll back", len(polluted))

        session = requests.Session()
        operations: list[UpdateOne] = []

        for artist_id, person_id in polluted:
            aka = _fetch_aka(session, args.api_key, person_id, args.timeout)
            normalized = [_normalize_name(a) for a in aka]
            normalized = [n for n in normalized if n]

            update: dict = {"$unset": {"tmdb_person_id": ""}}
            if aka:
                update["$pullAll"] = {
                    "aliases": aka,
                    "normalized_aliases": normalized,
                }
            operations.append(UpdateOne({"_id": artist_id}, update))
            logger.info(
                "queued rollback for artist _id=%s (person_id=%s, %d aliases to pull)",
                artist_id, person_id, len(aka),
            )

        if not args.apply:
            logger.info("DRY-RUN: would issue %d updates (pass --apply)", len(operations))
            return 0

        try:
            res = coll.bulk_write(operations, ordered=False)
            logger.info("rollback committed: matched=%d modified=%d", res.matched_count, res.modified_count)
        except PyMongoError as exc:
            logger.error("bulk_write failed: %s", exc)
            return 1
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
