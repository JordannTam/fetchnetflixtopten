"""Netflix Top 10 -> TMDB -> tracked-artist scoring pipeline.

Single-pass script (happy path, no retry):
  1. Fetch Netflix's weekly TSV (latest week, 18 tracked countries)
  2. Resolve each unique title -> TMDB id (search/movie|tv)
  3. Fetch each title's cast (TMDB credits)
  4. Match cast to tracked artists by tmdb_person_id
  5. Score per artist: dedupe by title across countries (max), sum across distinct titles
  6. Dump results to out/artist_scores.json

Required env: TMDB_API_KEY, ARTISTS_MONGODB_URI (or MONGODB_URI).
Run: python3 scripts/scrape_netflix_top10.py
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

import requests
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from requests.exceptions import RequestException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scrape_netflix")

ARTIST_DP_COLLECTION = "artist_dp_netflix"

TSV_URL = "https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv"
TMDB_BASE = "https://api.themoviedb.org/3"
TRACKED_COUNTRIES = frozenset({
    "South Korea", "Hong Kong", "Taiwan", "Japan", "Thailand", "Vietnam",
    "Philippines", "Indonesia", "United States", "Canada", "Brazil", "Mexico",
    "United Kingdom", "Germany", "France", "Spain", "Italy", "Australia",
})


def fetch_tsv() -> list[dict]:
    r = requests.get(TSV_URL, timeout=30)
    r.raise_for_status()
    return list(csv.DictReader(io.StringIO(r.text), delimiter="\t"))


def filter_rows(rows: list[dict]) -> tuple[str, list[dict]]:
    weeks = [r["week"] for r in rows if r.get("week")]
    if not weeks:
        return "", []
    latest_week = max(weeks)
    out: list[dict] = []
    for r in rows:
        if r.get("week") != latest_week or r.get("country_name") not in TRACKED_COUNTRIES:
            continue
        category = "tv" if r.get("category", "").lower().startswith("tv") else "movie"
        # show_title is the canonical title for both films and TV series.
        # season_title can be "N/A" (films) or season-specific (e.g. "Squid Game: Season 3"),
        # which doesn't match TMDB's series naming -- so we always search by show_title.
        title = (r.get("show_title") or "").strip()
        if not title:
            continue
        out.append({
            "country": r["country_name"],
            "category": category,
            "rank": int(r["weekly_rank"]),
            "title": title,
            "weeks_in_top10": int(r.get("cumulative_weeks_in_top_10") or 0),
            "hours_viewed": int(r.get("weekly_hours_viewed") or 0),
            "week": latest_week,
        })
    return latest_week, out


def resolve_tmdb(
    session: requests.Session, api_key: str, title: str, category: str, timeout: float
) -> tuple[int, str, int | None] | None:
    """Returns (tmdb_id, media_type, release_year) for the first search hit, or None."""
    media_type = "tv" if category == "tv" else "movie"
    try:
        r = session.get(
            f"{TMDB_BASE}/search/{media_type}",
            params={"api_key": api_key, "query": title, "include_adult": "false"},
            timeout=timeout,
        )
        r.raise_for_status()
        results = r.json().get("results", [])
    except RequestException as exc:
        logger.warning("TMDB search failed for '%s' (%s): %s", title, media_type, exc)
        return None
    if not results:
        return None
    hit = results[0]
    date_str = hit.get("release_date") if media_type == "movie" else hit.get("first_air_date")
    release_year = int(date_str[:4]) if date_str and len(date_str) >= 4 and date_str[:4].isdigit() else None
    return hit["id"], media_type, release_year


def fetch_cast(
    session: requests.Session, api_key: str, tmdb_id: int, media_type: str, timeout: float
) -> list[dict]:
    try:
        r = session.get(
            f"{TMDB_BASE}/{media_type}/{tmdb_id}/credits",
            params={"api_key": api_key},
            timeout=timeout,
        )
        r.raise_for_status()
        return r.json().get("cast", []) or []
    except RequestException as exc:
        logger.warning("TMDB credits failed for %s/%s: %s", media_type, tmdb_id, exc)
        return []


def load_artist_index(uri: str, db_name: str) -> dict[int, dict]:
    """Build {tmdb_person_id: artist}. Skips artists without tmdb_person_id."""
    client = MongoClient(uri)
    try:
        cursor = client[db_name]["artists"].find(
            {"tmdb_person_id": {"$exists": True}, "type": "Actor"},
            {"_id": 1, "tenant_id": 1, "english_name": 1, "korean_name": 1, "tmdb_person_id": 1},
        )
        return {a["tmdb_person_id"]: a for a in cursor}
    finally:
        client.close()


def score_matches(matches: list[dict]) -> list[dict]:
    """Per-artist: max DP per (artist, title) across countries, then sum across titles.

    Returns one doc per artist with embedded drama breakdown:
      {
        artist_id, tenant_id, english_name, korean_name, week,
        dp_netflix,                       # total across distinct titles
        dramas: [{tmdb_id, title, contribution}, ...]
      }
    """
    by_artist_title: dict[tuple[str, int], dict] = {}
    artist_meta: dict[str, dict] = {}

    for m in matches:
        # Rank Score: rank 1 = 200, rank 2 = 199, ..., rank 10 = 191.
        # DP_Netflix = Rank Score + (Weeks on Chart * 10) + (Streamed Hours / 1,000,000).
        rank_score = max(201 - m["rank"], 0)
        dp = rank_score + m["weeks_in_top10"] * 10 + m["hours_viewed"] / 1_000_000
        artist_id = str(m["artist_id"])
        key = (artist_id, m["tmdb_id"])
        existing = by_artist_title.get(key)
        if existing is None or dp > existing["contribution"]:
            by_artist_title[key] = {
                "tmdb_id": m["tmdb_id"],
                "title": m["title"],
                "release_year": m.get("release_year"),
                "contribution": dp,
            }
        artist_meta[artist_id] = m

    artist_dramas: defaultdict[str, list[dict]] = defaultdict(list)
    for (artist_id, _), drama in by_artist_title.items():
        artist_dramas[artist_id].append(drama)

    out: list[dict] = []
    for artist_id, dramas in artist_dramas.items():
        total = sum(d["contribution"] for d in dramas)
        meta = artist_meta[artist_id]
        week_date = meta["week"]
        # ISO week number: 1..52 (or 53). isocalendar().year handles edge cases
        # where a date in early Jan or late Dec belongs to the prior/next ISO year.
        iso = datetime.strptime(week_date, "%Y-%m-%d").date().isocalendar()
        out.append({
            "artist_id": meta["artist_id"],          # ObjectId
            "tenant_id": meta.get("tenant_id"),      # ObjectId or None
            "english_name": meta.get("english_name"),
            "korean_name": meta.get("korean_name"),
            "year": iso.year,
            "week": iso.week,
            "week_date": week_date,                  # original chart-end date (Sunday)
            "dp_netflix": round(total, 2),
            "dramas": [
                {
                    "tmdb_id": d["tmdb_id"],
                    "title": d["title"],
                    "release_year": d.get("release_year"),
                    "contribution": round(d["contribution"], 2),
                }
                for d in sorted(dramas, key=lambda x: -x["contribution"])
            ],
        })
    return sorted(out, key=lambda x: -x["dp_netflix"])


def upsert_scores(scores: list[dict], uri: str, db_name: str) -> dict:
    """Bulk upsert into general.artist_dp_netflix keyed by (tenant_id, artist_id, year, week).

    Idempotent: re-running the same (year, week) overwrites the doc.
    """
    if not scores:
        return {"upserted": 0, "modified": 0, "matched": 0}

    now = datetime.now(timezone.utc)
    client = MongoClient(uri)
    try:
        coll = client[db_name][ARTIST_DP_COLLECTION]
        # Idempotent: create_index is a no-op if the index already exists.
        coll.create_index(
            [("tenant_id", 1), ("artist_id", 1), ("year", 1), ("week", 1)],
            unique=True,
            name="tenant_artist_year_week_unique",
        )
        operations = [
            UpdateOne(
                {
                    "tenant_id": s["tenant_id"],
                    "artist_id": s["artist_id"],
                    "year": s["year"],
                    "week": s["week"],
                },
                {"$set": {**s, "updated_at": now}},
                upsert=True,
            )
            for s in scores
        ]
        result = coll.bulk_write(operations, ordered=False)
        return {
            "upserted": result.upserted_count,
            "modified": result.modified_count,
            "matched": result.matched_count,
        }
    finally:
        client.close()


def run(api_key: str, mongo_uri: str, db_name: str, timeout: float) -> dict:
    logger.info("loading artist index from %s.artists", db_name)
    try:
        by_tmdb_id = load_artist_index(mongo_uri, db_name)
    except PyMongoError as exc:
        logger.error("MongoDB load failed: %s", exc)
        raise
    logger.info("loaded %d Actor artists with tmdb_person_id", len(by_tmdb_id))

    logger.info("fetching Netflix TSV")
    week, entries = filter_rows(fetch_tsv())
    if not entries:
        logger.error("no entries after filtering -- aborting")
        return {"week": "", "entries": 0, "matches": 0, "scored_artists": 0}
    logger.info(
        "week=%s entries=%d countries=%d",
        week, len(entries), len({e["country"] for e in entries}),
    )

    session = requests.Session()
    title_cache: dict[tuple[str, str], tuple[int, str, int | None] | None] = {}
    cast_cache: dict[int, list[dict]] = {}
    matches: list[dict] = []

    for entry in entries:
        cache_key = (entry["title"], entry["category"])
        if cache_key not in title_cache:
            title_cache[cache_key] = resolve_tmdb(
                session, api_key, entry["title"], entry["category"], timeout,
            )
        resolved = title_cache[cache_key]
        if not resolved:
            continue
        tmdb_id, media_type, release_year = resolved
        if tmdb_id not in cast_cache:
            cast_cache[tmdb_id] = fetch_cast(session, api_key, tmdb_id, media_type, timeout)
        for member in cast_cache[tmdb_id]:
            artist = by_tmdb_id.get(member.get("id"))
            if artist:
                matches.append({
                    **entry,
                    "tmdb_id": tmdb_id,
                    "release_year": release_year,
                    "artist_id": artist["_id"],
                    "tenant_id": artist.get("tenant_id"),
                    "english_name": artist.get("english_name"),
                    "korean_name": artist.get("korean_name"),
                })

    resolved_count = sum(1 for v in title_cache.values() if v)
    logger.info(
        "resolved %d/%d unique titles, fetched cast for %d, %d cast-to-artist matches",
        resolved_count, len(title_cache), len(cast_cache), len(matches),
    )

    scores = score_matches(matches)
    return {
        "week": week,
        "entries": len(entries),
        "unique_titles": len(title_cache),
        "resolved_titles": resolved_count,
        "matches": len(matches),
        "scored_artists": len(scores),
        "scores": scores,
    }


def _to_jsonable(value):
    """Convert ObjectId / unknown types to str for pretty-printing."""
    if isinstance(value, dict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    return str(value) if value.__class__.__name__ == "ObjectId" else value


def print_samples(scores: list[dict], n: int = 3) -> None:
    """Print n sample documents in the proposed Mongo doc shape."""
    print()
    print("=" * 70)
    print(f"Sample of {min(n, len(scores))} documents (proposed shape for general.artist_dp_netflix)")
    print("=" * 70)
    for i, score in enumerate(scores[:n], 1):
        print(f"\n--- Sample {i} ---")
        print(json.dumps(_to_jsonable(score), indent=2, ensure_ascii=False))
    print()


def lambda_handler(event, context):  # noqa: ARG001 -- AWS Lambda signature
    """AWS Lambda entry point. Always commits to MongoDB."""
    api_key = os.environ["TMDB_API_KEY"]
    mongo_uri = os.environ.get("ARTISTS_MONGODB_URI") or os.environ["MONGODB_URI"]
    db_name = os.environ.get("ARTISTS_SOURCE_DATABASE", "general")
    timeout = float(os.environ.get("TMDB_TIMEOUT", "10"))

    result = run(api_key, mongo_uri, db_name, timeout)
    scores = result.pop("scores", [])
    write_stats = upsert_scores(scores, mongo_uri, db_name)
    summary = {**result, **write_stats}
    logger.info("lambda done: %s", summary)
    return {"statusCode": 200, "body": summary}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api-key", default=os.environ.get("TMDB_API_KEY"))
    parser.add_argument("--uri", default=os.environ.get("ARTISTS_MONGODB_URI") or os.environ.get("MONGODB_URI"))
    parser.add_argument("--db", default=os.environ.get("ARTISTS_SOURCE_DATABASE", "general"))
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--samples", type=int, default=3, help="how many sample docs to print")
    parser.add_argument("--apply", action="store_true", help="commit upserts (default: dry-run preview)")
    args = parser.parse_args()

    if not args.api_key:
        logger.error("TMDB_API_KEY is required (env or --api-key)")
        return 2
    if not args.uri:
        logger.error("Mongo URI is required (ARTISTS_MONGODB_URI / MONGODB_URI or --uri)")
        return 2

    result = run(args.api_key, args.uri, args.db, args.timeout)
    scores = result.pop("scores", [])
    print_samples(scores, n=args.samples)

    if args.apply:
        try:
            write_stats = upsert_scores(scores, args.uri, args.db)
        except PyMongoError as exc:
            logger.error("upsert failed: %s", exc)
            return 1
        result.update(write_stats)
        logger.info("done (committed): %s", result)
    else:
        logger.info("DRY-RUN: %d scores ready to upsert (pass --apply to commit)", len(scores))
        logger.info("done: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
