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
from src.storage.mongo_client import get_external_database

logger = logging.getLogger(__name__)


def _write_count(result: object) -> int:
    upserted = int(getattr(result, "upserted_count", 0) or 0)
    modified = int(getattr(result, "modified_count", 0) or 0)
    return upserted + modified


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
        artists_db_name = config.artists_source_database
        output_db_name = config.database if config.linking_test_mode else config.general_database

        artists_db = db.client[artists_db_name]
        if config.artists_source_uri:
            artists_db = get_external_database(
                uri=config.artists_source_uri,
                database=artists_db_name,
                max_pool_size=config.max_pool_size,
            )
        output_db = db.client[output_db_name]
        self._artists = artists_db[config.artists_collection]
        self._content_catalog = output_db[config.content_catalog_collection]
        self._content_links = output_db[config.content_links_collection]
        self._artist_drama_scores = output_db[config.artist_drama_score_collection]
        self._review_queue = output_db[config.review_queue_collection]

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
        content_index = IndexModel(
            [("provider", ASCENDING), ("provider_content_id", ASCENDING)],
            unique=True,
            name="provider_content_unique",
        )
        link_index = IndexModel(
            [
                ("tenant_id", ASCENDING),
                ("content_provider", ASCENDING),
                ("content_id", ASCENDING),
                ("artist_id", ASCENDING),
                ("role", ASCENDING),
            ],
            unique=True,
            name="tenant_content_artist_role_unique",
        )
        score_index = IndexModel(
            [
                ("tenant_id", ASCENDING),
                ("artist_id", ASCENDING),
                ("year", ASCENDING),
                ("week", ASCENDING),
                ("country", ASCENDING),
            ],
            unique=True,
            name="tenant_artist_year_week_country_unique",
        )
        review_index = IndexModel(
            [
                ("tenant_id", ASCENDING),
                ("week", ASCENDING),
                ("country", ASCENDING),
                ("category", ASCENDING),
                ("credit_name", ASCENDING),
                ("candidate_artist_id", ASCENDING),
            ],
            unique=True,
            name="review_match_unique",
        )
        self._content_catalog.create_indexes([content_index])
        self._content_links.create_indexes([link_index])
        self._artist_drama_scores.create_indexes([score_index])
        self._review_queue.create_indexes([review_index])
        logger.info("Ensured indexes on rankings collection")

    def save_rankings(
        self,
        rankings: tuple[CountryRanking, ...],
        mongo_session=None,
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

        result = self._rankings.bulk_write(
            operations,
            ordered=False,
            session=mongo_session,
        )
        saved = _write_count(result)
        logger.info(
            "Saved %d rankings (%d upserted, %d modified)",
            saved,
            result.upserted_count,
            result.modified_count,
        )
        return saved

    def save_scrape_run(self, run: ScrapeRun, mongo_session=None) -> None:
        """Insert an audit record for this Lambda invocation.

        Args:
            run: ScrapeRun with status, timing, and error details.
        """
        self._runs.insert_one(run.to_document(), session=mongo_session)
        logger.info("Saved scrape run %s", run.run_id)

    def load_tracked_artists(self, mongo_session=None) -> tuple[dict, ...]:
        cursor = self._artists.find(
            {"is_active": {"$ne": False}},
            {
                "artist_id": 1,
                "english_name": 1,
                "korean_name": 1,
                "aliases": 1,
                "normalized_aliases": 1,
                "type": 1,
                "image": 1,
                "image_url": 1,
                "tenant_id": 1,
            },
            session=mongo_session,
        )
        return tuple(cursor)

    def save_content_catalog(self, docs: tuple[dict, ...], mongo_session=None) -> int:
        if not docs:
            return 0
        operations = [
            UpdateOne(
                {
                    "provider": doc["provider"],
                    "provider_content_id": doc["provider_content_id"],
                },
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
        ]
        result = self._content_catalog.bulk_write(
            operations,
            ordered=False,
            session=mongo_session,
        )
        return _write_count(result)

    def save_content_artist_links(self, docs: tuple[dict, ...], mongo_session=None) -> int:
        if not docs:
            return 0
        operations = [
            UpdateOne(
                {
                    "tenant_id": doc.get("tenant_id"),
                    "content_provider": doc["content_provider"],
                    "content_id": doc["content_id"],
                    "artist_id": doc["artist_id"],
                    "role": doc["role"],
                },
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
        ]
        result = self._content_links.bulk_write(
            operations,
            ordered=False,
            session=mongo_session,
        )
        return _write_count(result)

    def save_artist_drama_scores(self, docs: tuple[dict, ...], mongo_session=None) -> int:
        if not docs:
            return 0
        operations = [
            UpdateOne(
                {
                    "tenant_id": doc.get("tenant_id"),
                    "artist_id": doc["artist_id"],
                    "year": doc["year"],
                    "week": doc["week"],
                    "country": doc["country"],
                },
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
        ]
        result = self._artist_drama_scores.bulk_write(
            operations,
            ordered=False,
            session=mongo_session,
        )
        return _write_count(result)

    def save_match_reviews(self, docs: tuple[dict, ...], mongo_session=None) -> int:
        if not docs:
            return 0
        operations = [
            UpdateOne(
                {
                    "tenant_id": doc.get("tenant_id"),
                    "week": doc["week"],
                    "country": doc["country"],
                    "category": doc["category"],
                    "credit_name": doc["credit_name"],
                    "candidate_artist_id": doc["candidate_artist_id"],
                },
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
        ]
        result = self._review_queue.bulk_write(
            operations,
            ordered=False,
            session=mongo_session,
        )
        return _write_count(result)
