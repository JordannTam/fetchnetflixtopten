"""One-time migration: replace string artist_id with ObjectId from artists._id.

Affects three collections in the `general` database:
  - netflix_top10_content_artist_links.artist_id
  - netflix_top10_artist_drama_score.artist_id
  - netflix_top10_match_review_queue.candidate_artist_id

Mapping is built from `general.artists` (artist_id -> _id).

Dry-run by default. Pass --apply to commit changes.

DEPLOY ORDERING (important):
  This migration MUST run BEFORE deploying the new Lambda that writes ObjectId
  artist_ids. Running it after new writes have landed risks index conflicts on
  the unique compound indexes (tenant_content_artist_role_unique etc.) when a
  migrated row collides with a freshly-written ObjectId row. The script
  pre-flight aborts if it detects a target collection already contains BOTH
  string and ObjectId values for the migration field.

Usage:
  python3 scripts/migrate_artist_id_to_objectid.py \
      --uri "$ARTISTS_MONGODB_URI" \
      --db general
  python3 scripts/migrate_artist_id_to_objectid.py --apply ...
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

from bson import ObjectId
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

# Make repo root importable so this script can be run directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import MongoConfig  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("migrate_artist_id")

# Sourced from MongoConfig defaults to keep collection names in sync with the app.
_DEFAULT_CONFIG = MongoConfig()
TARGET_COLLECTIONS: tuple[tuple[str, str], ...] = (
    (_DEFAULT_CONFIG.content_links_collection, "artist_id"),
    (_DEFAULT_CONFIG.artist_drama_score_collection, "artist_id"),
    (_DEFAULT_CONFIG.review_queue_collection, "candidate_artist_id"),
)
ARTISTS_COLLECTION = _DEFAULT_CONFIG.artists_collection


def build_id_map(db) -> dict[str, ObjectId]:
    """Build a {artist_id (str): _id (ObjectId)} map from the artists collection."""
    mapping: dict[str, ObjectId] = {}
    for doc in db[ARTISTS_COLLECTION].find({}, {"_id": 1, "artist_id": 1}):
        legacy_id = doc.get("artist_id")
        if legacy_id is None:
            continue
        key = str(legacy_id)
        if key in mapping:
            logger.warning(
                "Duplicate artist_id %s in artists collection (prev _id=%s, new _id=%s) — using newer",
                key, mapping[key], doc["_id"],
            )
        mapping[key] = doc["_id"]
    logger.info("Loaded %d artist_id -> _id mappings from general.artists", len(mapping))
    return mapping


def preflight_check_no_mixed_types(db) -> bool:
    """Abort if any target collection already contains BOTH string and ObjectId
    values for the migration field. Mixed state means new code already wrote
    ObjectId rows alongside legacy string rows, and rewriting the strings risks
    colliding with existing ObjectId rows on the unique compound indexes.

    Returns True if safe to proceed, False if mixed state detected.
    """
    safe = True
    for collection_name, field_name in TARGET_COLLECTIONS:
        coll = db[collection_name]
        has_string = coll.find_one({field_name: {"$type": "string"}}, {"_id": 1}) is not None
        has_objectid = coll.find_one({field_name: {"$type": "objectId"}}, {"_id": 1}) is not None
        if has_string and has_objectid:
            logger.error(
                "[%s] PREFLIGHT FAIL: %s contains both string and ObjectId values. "
                "Migrating now risks index collisions. Investigate manually before retrying.",
                collection_name, field_name,
            )
            safe = False
        elif has_string:
            logger.info("[%s] preflight OK: only string %s values present", collection_name, field_name)
        elif has_objectid:
            logger.info(
                "[%s] preflight OK: %s already migrated (ObjectId only)",
                collection_name, field_name,
            )
        else:
            logger.info("[%s] preflight OK: collection empty", collection_name)
    return safe


def migrate_collection(
    db,
    collection_name: str,
    field_name: str,
    id_map: dict[str, ObjectId],
    apply: bool,
) -> dict[str, int]:
    """Find docs whose `field_name` is a string and rewrite to ObjectId."""
    coll = db[collection_name]
    cursor = coll.find(
        {field_name: {"$type": "string"}},
        {"_id": 1, field_name: 1},
    )

    operations: list[UpdateOne] = []
    skipped_unknown = 0
    skipped_already_objectid = 0
    candidates = 0

    for doc in cursor:
        candidates += 1
        legacy_value = doc.get(field_name)
        if not isinstance(legacy_value, str):
            skipped_already_objectid += 1
            continue
        new_oid = id_map.get(legacy_value)
        if new_oid is None:
            skipped_unknown += 1
            logger.warning(
                "[%s] %s=%s has no match in artists.artist_id (doc _id=%s)",
                collection_name, field_name, legacy_value, doc["_id"],
            )
            continue
        operations.append(
            UpdateOne({"_id": doc["_id"]}, {"$set": {field_name: new_oid}})
        )

    stats = {
        "candidates": candidates,
        "to_update": len(operations),
        "skipped_unknown": skipped_unknown,
        "skipped_non_string": skipped_already_objectid,
        "modified": 0,
    }

    if not operations:
        logger.info("[%s] no rows to update", collection_name)
        return stats

    if not apply:
        logger.info(
            "[%s] DRY-RUN: would update %d / %d docs (skipped: %d unknown)",
            collection_name, len(operations), candidates, skipped_unknown,
        )
        return stats

    result = coll.bulk_write(operations, ordered=False)
    stats["modified"] = result.modified_count
    logger.info(
        "[%s] APPLIED: modified %d / %d docs (skipped: %d unknown)",
        collection_name, result.modified_count, candidates, skipped_unknown,
    )
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True, help="MongoDB connection URI")
    parser.add_argument("--db", default="general", help="Database name (default: general)")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually perform writes (default is dry-run).",
    )
    args = parser.parse_args()

    if not args.apply:
        logger.warning("DRY-RUN mode — no writes will be performed. Pass --apply to commit.")

    client: MongoClient[Any] | None = None
    try:
        client = MongoClient(args.uri)
        db = client[args.db]

        if not preflight_check_no_mixed_types(db):
            logger.error("Aborting due to preflight failure. No changes made.")
            return 3

        id_map = build_id_map(db)

        if not id_map:
            logger.error("No artist_id -> _id mappings found. Aborting.")
            return 2

        all_stats: dict[str, dict[str, int]] = {}
        for collection_name, field_name in TARGET_COLLECTIONS:
            all_stats[collection_name] = migrate_collection(
                db, collection_name, field_name, id_map, args.apply,
            )

        logger.info("=" * 60)
        logger.info("SUMMARY (%s)", "APPLY" if args.apply else "DRY-RUN")
        for name, stats in all_stats.items():
            logger.info(
                "  %s: candidates=%d to_update=%d unknown=%d modified=%d",
                name, stats["candidates"], stats["to_update"],
                stats["skipped_unknown"], stats["modified"],
            )
    except PyMongoError as exc:
        logger.error("Migration failed: %s", exc)
        return 1
    finally:
        if client is not None:
            try:
                client.close()
            except PyMongoError as exc:
                logger.warning("Error closing MongoDB client: %s", exc)

    return 0


if __name__ == "__main__":
    sys.exit(main())
