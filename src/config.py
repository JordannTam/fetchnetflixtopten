"""Configuration for Netflix Top 10 scraper.

Centralizes all configuration: tracked countries, Netflix API settings,
and MongoDB connection parameters. All config is loaded from environment
variables at runtime (no hardcoded secrets).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse

TRACKED_COUNTRIES: dict[str, str] = {
    "South Korea": "south-korea",
    "Hong Kong": "hong-kong",
    "Taiwan": "taiwan",
    "Japan": "japan",
    "Thailand": "thailand",
    "Vietnam": "vietnam",
    "Philippines": "philippines",
    "Indonesia": "indonesia",
    "United States": "united-states",
    "Canada": "canada",
    "Brazil": "brazil",
    "Mexico": "mexico",
    "United Kingdom": "united-kingdom",
    "Germany": "germany",
    "France": "france",
    "Spain": "spain",
    "Italy": "italy",
    "Australia": "australia",
}


@dataclass(frozen=True)
class NetflixConfig:
    base_url: str = "https://www.netflix.com/tudum/top10"
    user_agent: str = "NetflixTop10Collector/1.0"
    request_timeout: int = 30
    retry_count: int = 3
    html_delay: float = 1.5
    tmdb_base_url: str = "https://api.themoviedb.org/3"
    tmdb_api_key: str = ""
    tmdb_timeout: int = 15
    fuzzy_review_threshold: float = 0.85
    weeks_top10_cap: int = 10  # cap longevity bonus at this many weeks (10 weeks = 100 pts max)


@dataclass(frozen=True)
class MongoConfig:
    uri: str = ""
    database: str = "netflix_top10"
    rankings_collection: str = "netflix_top10_weekly_rankings"
    runs_collection: str = "netflix_top10_scrape_runs"
    general_database: str = "general"
    artists_collection: str = "artists"
    content_catalog_collection: str = "netflix_top10_content_catalog"
    content_links_collection: str = "netflix_top10_content_artist_links"
    artist_drama_score_collection: str = "netflix_top10_artist_drama_score"
    review_queue_collection: str = "netflix_top10_match_review_queue"
    linking_test_mode: bool = False
    artists_source_database: str = "general"
    artists_source_uri: str = ""
    max_pool_size: int = 1


def _parse_bool(value: str | None) -> bool:
    normalized = (value or "").strip().lower()
    return normalized in {"1", "true", "yes", "on"}


def load_config() -> tuple[NetflixConfig, MongoConfig]:
    """Load and validate configuration from environment variables.

    Reads MONGODB_URI from the environment, validates it has a valid
    MongoDB scheme (mongodb:// or mongodb+srv://), and returns frozen
    config objects for Netflix and MongoDB settings.

    Returns:
        Tuple of (NetflixConfig, MongoConfig) with validated settings.

    Raises:
        ValueError: If MONGODB_URI is missing or has an invalid scheme.
    """
    mongo_uri = os.environ.get("MONGODB_URI", "")
    if not mongo_uri:
        raise ValueError("MONGODB_URI environment variable is required")

    parsed = urlparse(mongo_uri)
    if parsed.scheme not in ("mongodb", "mongodb+srv"):
        raise ValueError(
            f"Invalid MongoDB URI scheme: '{parsed.scheme}'. "
            "Expected 'mongodb' or 'mongodb+srv'."
        )

    return (
        NetflixConfig(
            tmdb_api_key=os.environ.get("TMDB_API_KEY", "").strip(),
        ),
        MongoConfig(
            uri=mongo_uri,
            linking_test_mode=_parse_bool(os.environ.get("LINKING_TEST_MODE")),
            artists_source_database=(
                os.environ.get("ARTISTS_SOURCE_DATABASE", "general").strip()
                or "general"
            ),
            artists_source_uri=os.environ.get("ARTISTS_MONGODB_URI", "").strip(),
        ),
    )
