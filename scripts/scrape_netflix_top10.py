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
from pathlib import Path

import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from requests.exceptions import RequestException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("scrape_netflix")

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
) -> tuple[int, str] | None:
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
    return (results[0]["id"], media_type) if results else None


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
    """Per-artist: max DP per (artist, title) across countries, then sum across titles."""
    by_artist_title: dict[tuple[str, int], float] = {}
    artist_meta: dict[str, dict] = {}
    artist_titles: defaultdict[str, set[str]] = defaultdict(set)

    for m in matches:
        # Rank Score: rank 1 = 200, rank 2 = 199, ..., rank 10 = 191.
        # DP_Netflix = Rank Score + (Weeks on Chart * 10) + (Streamed Hours / 1,000,000).
        rank_score = max(201 - m["rank"], 0)
        dp = rank_score + m["weeks_in_top10"] * 10 + m["hours_viewed"] / 1_000_000
        artist_id = str(m["artist_id"])
        key = (artist_id, m["tmdb_id"])
        if dp > by_artist_title.get(key, 0):
            by_artist_title[key] = dp
        artist_meta[artist_id] = m
        artist_titles[artist_id].add(m["title"])

    totals: defaultdict[str, float] = defaultdict(float)
    for (artist_id, _), dp in by_artist_title.items():
        totals[artist_id] += dp

    return [
        {
            "artist_id": artist_id,
            "tenant_id": str(artist_meta[artist_id].get("tenant_id", "")),
            "english_name": artist_meta[artist_id].get("english_name"),
            "korean_name": artist_meta[artist_id].get("korean_name"),
            "week": artist_meta[artist_id]["week"],
            "dp_netflix": round(total, 2),
            "matched_titles": sorted(artist_titles[artist_id]),
        }
        for artist_id, total in sorted(totals.items(), key=lambda kv: -kv[1])
    ]


def run(api_key: str, mongo_uri: str, db_name: str, output_path: Path, timeout: float) -> dict:
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
    title_cache: dict[tuple[str, str], tuple[int, str] | None] = {}
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
        tmdb_id, media_type = resolved
        if tmdb_id not in cast_cache:
            cast_cache[tmdb_id] = fetch_cast(session, api_key, tmdb_id, media_type, timeout)
        for member in cast_cache[tmdb_id]:
            artist = by_tmdb_id.get(member.get("id"))
            if artist:
                matches.append({
                    **entry,
                    "tmdb_id": tmdb_id,
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
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as f:
        json.dump({"week": week, "scores": scores}, f, indent=2, ensure_ascii=False, default=str)
    logger.info("wrote %d artist scores -> %s", len(scores), output_path)

    return {
        "week": week,
        "entries": len(entries),
        "unique_titles": len(title_cache),
        "resolved_titles": resolved_count,
        "matches": len(matches),
        "scored_artists": len(scores),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api-key", default=os.environ.get("TMDB_API_KEY"))
    parser.add_argument("--uri", default=os.environ.get("ARTISTS_MONGODB_URI") or os.environ.get("MONGODB_URI"))
    parser.add_argument("--db", default=os.environ.get("ARTISTS_SOURCE_DATABASE", "general"))
    parser.add_argument("--output", default="out/artist_scores.json")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    if not args.api_key:
        logger.error("TMDB_API_KEY is required (env or --api-key)")
        return 2
    if not args.uri:
        logger.error("Mongo URI is required (ARTISTS_MONGODB_URI / MONGODB_URI or --uri)")
        return 2

    stats = run(args.api_key, args.uri, args.db, Path(args.output), args.timeout)
    logger.info("done: %s", stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
