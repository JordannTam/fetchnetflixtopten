"""Primary data source: Netflix's public TSV export.

Netflix publishes a tab-separated file containing weekly Top 10 rankings
for every country they track (~94 countries, 240+ weeks of history).
This is far more stable than HTML scraping because:

1. It's a structured data export, not a rendered web page
2. No CSS class names or DOM structure to break on redesign
3. Contains all countries and weeks in a single ~28MB download
4. Updated weekly by Netflix alongside their website

The TSV columns are:
    country_name, country_iso2, week, category, weekly_rank,
    show_title, season_title, cumulative_weeks_in_top_10

We filter this down to only the 18 TRACKED_COUNTRIES defined in config,
and only the latest week (unless a specific week is requested).
"""

from __future__ import annotations

import csv
import logging
from collections import defaultdict
from io import StringIO

import requests

from src.config import TRACKED_COUNTRIES, NetflixConfig
from src.models import CountryRanking, RankingEntry

logger = logging.getLogger(__name__)

COUNTRIES_TSV_URL = (
    "https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv"
)

CATEGORY_MAP = {
    "Films": "films",
    "TV": "tv",
}


def _country_slug(name: str) -> str:
    """Convert a country name to a URL-safe slug.

    Examples:
        "United States" -> "united-states"
        "South Korea"   -> "south-korea"
    """
    return name.lower().replace(" ", "-").replace(".", "")


def _parse_int(value: str) -> int:
    """Safely parse a string to int, returning 0 on failure.

    The TSV data sometimes has empty or malformed numeric fields,
    so this avoids crashing the entire parse on one bad row.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def fetch_latest_week(
    session: requests.Session,
    config: NetflixConfig,
) -> tuple[CountryRanking, ...]:
    """Download the full TSV and parse only the most recent week.

    Downloads the entire ~28MB TSV file (all countries, all weeks),
    then filters to the latest week and the 18 tracked countries.
    This is the primary data source for the scraper.

    Args:
        session: HTTP session with retry strategy.
        config: Netflix configuration with timeout settings.

    Returns:
        Tuple of CountryRanking objects (one per country-category combo).
        For 18 countries x 2 categories = up to 36 results.

    Raises:
        requests.HTTPError: If the TSV download returns a non-200 status.
    """
    logger.info("Fetching countries TSV from Netflix")
    response = session.get(
        COUNTRIES_TSV_URL,
        timeout=config.request_timeout,
    )
    response.raise_for_status()

    return _parse_countries_tsv(response.text)


def fetch_specific_week(
    session: requests.Session,
    config: NetflixConfig,
    week: str,
) -> tuple[CountryRanking, ...]:
    """Download the full TSV and parse a specific week's data.

    Same as fetch_latest_week but filters to a specific week instead
    of auto-detecting the latest one. Useful for backfilling.

    Args:
        session: HTTP session with retry strategy.
        config: Netflix configuration with timeout settings.
        week: ISO date string (e.g. "2026-02-01") to filter by.

    Returns:
        Tuple of CountryRanking objects for the requested week.

    Raises:
        requests.HTTPError: If the TSV download returns a non-200 status.
    """
    logger.info("Fetching countries TSV for week %s", week)
    response = session.get(
        COUNTRIES_TSV_URL,
        timeout=config.request_timeout,
    )
    response.raise_for_status()

    return _parse_countries_tsv(response.text, target_week=week)


def _parse_countries_tsv(
    tsv_text: str,
    target_week: str | None = None,
) -> tuple[CountryRanking, ...]:
    """Parse the TSV text into CountryRanking objects.

    Two-pass approach:
    1. First pass: scan all rows, track the latest week (if no target),
       filter to tracked countries, group rows by (week, country, category).
    2. Second pass: if no target_week, discard everything except the
       latest week. Build sorted RankingEntry tuples for each group.

    Week comparison uses string comparison on ISO dates (YYYY-MM-DD),
    which sorts correctly because the format is fixed-width and
    lexicographic order matches chronological order.

    Args:
        tsv_text: Raw TSV content from Netflix.
        target_week: If set, only return data for this specific week.
            If None, auto-detect and return only the latest week.

    Returns:
        Tuple of CountryRanking objects, sorted by (week, country, category).
        Empty tuple if no matching data found.
    """
    reader = csv.DictReader(StringIO(tsv_text), delimiter="\t")

    grouped: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    latest_week = ""

    tracked_names = set(TRACKED_COUNTRIES.keys())

    for row in reader:
        week = row["week"]
        country_name = row["country_name"]
        if target_week is None:
            if week > latest_week:
                latest_week = week
        if target_week and week != target_week:
            continue
        if country_name not in tracked_names:
            continue
        grouped[(week, country_name, row["category"])].append(row)

    if target_week is None:
        grouped = {
            key: rows
            for key, rows in grouped.items()
            if key[0] == latest_week
        }

    rankings = []
    for (week, country_name, category), rows in sorted(grouped.items()):
        category_slug = CATEGORY_MAP.get(category, category.lower())
        entries = tuple(
            RankingEntry(
                rank=_parse_int(row["weekly_rank"]),
                title=row["show_title"],
                weeks_in_top_10=_parse_int(
                    row["cumulative_weeks_in_top_10"]
                ),
                hours_viewed=0,
            )
            for row in sorted(rows, key=lambda r: _parse_int(r["weekly_rank"]))
        )
        rankings.append(
            CountryRanking(
                week=week,
                country=_country_slug(country_name),
                country_name=country_name,
                category=category_slug,
                source="tsv",
                rankings=entries,
            )
        )

    logger.info(
        "Parsed %d country-category combinations from TSV",
        len(rankings),
    )
    return tuple(rankings)
