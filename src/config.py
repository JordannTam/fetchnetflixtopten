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


@dataclass(frozen=True)
class MongoConfig:
    uri: str = ""
    database: str = "netflix_top10"
    rankings_collection: str = "weekly_rankings"
    runs_collection: str = "scrape_runs"
    max_pool_size: int = 1


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
        NetflixConfig(),
        MongoConfig(uri=mongo_uri),
    )
