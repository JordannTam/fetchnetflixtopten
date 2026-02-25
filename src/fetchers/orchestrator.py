"""Fetcher orchestrator: TSV primary, HTML fallback.

This module implements the resilience strategy for data collection.
Rather than depending on a single source that could break (as the
original main.py did with HTML scraping), we use a fallback chain:

    1. Try the TSV export (fast, structured, stable)
    2. If that fails, fall back to HTML scraping (slower, fragile)
    3. If both fail, return an empty result with error details

The orchestrator never raises exceptions - it always returns a
ScrapeResult, even on total failure. Errors are collected in the
result's errors tuple for the handler to log and store.
"""

from __future__ import annotations

import logging

import requests

from src.config import NetflixConfig
from src.fetchers.html_fetcher import fetch_all_countries
from src.fetchers.tsv_fetcher import fetch_latest_week
from src.models import ScrapeResult

logger = logging.getLogger(__name__)


def fetch_rankings(
    session: requests.Session,
    config: NetflixConfig,
) -> ScrapeResult:
    """Fetch weekly rankings using TSV primary, HTML fallback.

    Attempts the TSV download first because it's a single HTTP request
    for all countries and categories (fast, ~28MB). If that fails for
    any reason (network error, Netflix removes the file, etc.), falls
    back to HTML scraping which makes 36 individual page requests
    (18 countries x 2 categories, ~54 seconds with rate limiting).

    Args:
        session: HTTP session with retry strategy.
        config: Netflix configuration with URLs and timeouts.

    Returns:
        ScrapeResult containing:
        - rankings: tuple of CountryRanking objects (may be empty)
        - source_used: "tsv", "html_fallback", or "none"
        - errors: tuple of error messages from failed attempts
    """
    errors: list[str] = []

    try:
        logger.info("Attempting primary source: TSV download")
        rankings = fetch_latest_week(session, config)
        if rankings:
            logger.info(
                "TSV fetch succeeded: %d country-category results",
                len(rankings),
            )
            return ScrapeResult(
                rankings=rankings,
                source_used="tsv",
                errors=tuple(errors),
            )
        errors.append("TSV fetch returned zero results")
    except Exception as exc:
        msg = f"TSV fetch failed: {exc}"
        logger.error(msg)
        errors.append(msg)

    try:
        logger.info("Falling back to HTML scraping")
        rankings = fetch_all_countries(session, config)
        if rankings:
            logger.info(
                "HTML fallback succeeded: %d country-category results",
                len(rankings),
            )
            return ScrapeResult(
                rankings=rankings,
                source_used="html_fallback",
                errors=tuple(errors),
            )
        errors.append("HTML fallback returned zero results")
    except Exception as exc:
        msg = f"HTML fallback failed: {exc}"
        logger.error(msg)
        errors.append(msg)

    logger.error("All fetch sources exhausted")
    return ScrapeResult(
        rankings=(),
        source_used="none",
        errors=tuple(errors),
    )
