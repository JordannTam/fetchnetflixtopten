from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.config import MongoConfig
from src.models import CountryRanking, RankingEntry, ScrapeRun
from src.storage.repository import RankingsRepository


def _make_config():
    return MongoConfig(
        uri="mongodb://localhost:27017",
        database="test_db",
    )


def _make_ranking():
    return CountryRanking(
        week="2026-02-01",
        country="united-states",
        country_name="United States",
        category="films",
        source="tsv",
        rankings=(
            RankingEntry(rank=1, title="Movie A", weeks_in_top_10=2),
            RankingEntry(rank=2, title="Movie B", weeks_in_top_10=1),
        ),
    )


class TestRankingsRepository:
    def test_save_rankings_calls_bulk_write(self):
        mock_db = MagicMock()
        config = _make_config()
        repo = RankingsRepository(mock_db, config)

        mock_result = MagicMock()
        mock_result.upserted_count = 1
        mock_result.modified_count = 0
        mock_db[config.rankings_collection].bulk_write.return_value = (
            mock_result
        )

        ranking = _make_ranking()
        saved = repo.save_rankings((ranking,))

        assert saved == 1
        mock_db[config.rankings_collection].bulk_write.assert_called_once()

    def test_save_rankings_empty(self):
        mock_db = MagicMock()
        config = _make_config()
        repo = RankingsRepository(mock_db, config)

        saved = repo.save_rankings(())
        assert saved == 0
        mock_db[config.rankings_collection].bulk_write.assert_not_called()

    def test_save_scrape_run(self):
        mock_db = MagicMock()
        config = _make_config()
        repo = RankingsRepository(mock_db, config)

        now = datetime.now(timezone.utc)
        run = ScrapeRun(
            run_id="test-123",
            started_at=now,
            completed_at=now,
            status="success",
            source_used="tsv",
            total_documents_saved=10,
        )

        repo.save_scrape_run(run)
        mock_db[config.runs_collection].insert_one.assert_called_once()

    def test_ensure_indexes(self):
        mock_db = MagicMock()
        config = _make_config()
        repo = RankingsRepository(mock_db, config)

        repo.ensure_indexes()
        mock_db[config.rankings_collection].create_indexes.assert_called_once()
