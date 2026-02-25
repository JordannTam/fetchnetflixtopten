"""MongoDB repository for rankings storage.

Handles all database writes using upsert semantics so that re-running
the scraper for the same week doesn't create duplicates. Uses a
compound unique index on (week, country, category) to enforce this
at the database level.

Also stores audit records (scrape_runs) so every Lambda invocation
is traceable - useful for debugging failures and monitoring health.
"""

from __future__ import annotations

import logging

from pymongo import ASCENDING, IndexModel, UpdateOne
from pymongo.database import Database

from src.config import MongoConfig
from src.models import CountryRanking, ScrapeRun

logger = logging.getLogger(__name__)


class RankingsRepository:
    """Repository for reading and writing Netflix Top 10 rankings.

    Wraps two MongoDB collections:
    - weekly_rankings: the actual ranking data (upserted)
    - scrape_runs: audit trail of Lambda invocations (appended)
    """

    def __init__(self, db: Database, config: MongoConfig) -> None:
        """Initialize with database handle and collection names from config."""
        self._rankings = db[config.rankings_collection]
        self._runs = db[config.runs_collection]

    def ensure_indexes(self) -> None:
        """Create the compound unique index if it doesn't already exist.

        Index: (week, country, category) - ensures one document per
        combination. This makes re-runs safe: the upsert in save_rankings
        will update existing documents instead of creating duplicates.
        """
        compound_index = IndexModel(
            [
                ("week", ASCENDING),
                ("country", ASCENDING),
                ("category", ASCENDING),
            ],
            unique=True,
            name="week_country_category_unique",
        )
        self._rankings.create_indexes([compound_index])
        logger.info("Ensured indexes on rankings collection")

    def save_rankings(
        self, rankings: tuple[CountryRanking, ...]
    ) -> int:
        """Upsert all rankings to MongoDB in a single bulk operation.

        Uses bulk_write with UpdateOne + upsert=True so that:
        - New (week, country, category) combos are inserted
        - Existing ones are updated with fresh data
        - The entire batch is sent in one network round-trip

        Args:
            rankings: Tuple of CountryRanking objects to save.

        Returns:
            Number of documents upserted or modified.
        """
        if not rankings:
            return 0

        operations = [
            UpdateOne(
                {
                    "week": ranking.week,
                    "country": ranking.country,
                    "category": ranking.category,
                },
                {"$set": ranking.to_document()},
                upsert=True,
            )
            for ranking in rankings
        ]

        result = self._rankings.bulk_write(operations, ordered=False)
        saved = result.upserted_count + result.modified_count
        logger.info(
            "Saved %d rankings (%d upserted, %d modified)",
            saved,
            result.upserted_count,
            result.modified_count,
        )
        return saved

    def save_scrape_run(self, run: ScrapeRun) -> None:
        """Insert an audit record for this Lambda invocation.

        Args:
            run: ScrapeRun with status, timing, and error details.
        """
        self._runs.insert_one(run.to_document())
        logger.info("Saved scrape run %s", run.run_id)
