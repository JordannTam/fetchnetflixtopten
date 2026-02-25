from datetime import datetime, timezone

from src.models import CountryRanking, RankingEntry, ScrapeResult, ScrapeRun


class TestRankingEntry:
    def test_to_document_with_hours(self):
        entry = RankingEntry(
            rank=1, title="Test Movie", weeks_in_top_10=3, hours_viewed=1000000
        )
        doc = entry.to_document()
        assert doc == {
            "rank": 1,
            "title": "Test Movie",
            "weeks_in_top_10": 3,
            "hours_viewed": 1000000,
        }

    def test_to_document_without_hours(self):
        entry = RankingEntry(rank=2, title="Test Show", weeks_in_top_10=1)
        doc = entry.to_document()
        assert doc == {
            "rank": 2,
            "title": "Test Show",
            "weeks_in_top_10": 1,
        }
        assert "hours_viewed" not in doc

    def test_frozen(self):
        entry = RankingEntry(rank=1, title="Frozen Test", weeks_in_top_10=1)
        try:
            entry.rank = 2
            assert False, "Should have raised FrozenInstanceError"
        except AttributeError:
            pass


class TestCountryRanking:
    def test_to_document(self):
        now = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
        ranking = CountryRanking(
            week="2026-02-01",
            country="united-states",
            country_name="United States",
            category="films",
            source="tsv",
            rankings=(
                RankingEntry(rank=1, title="Movie A", weeks_in_top_10=2),
            ),
            fetched_at=now,
        )
        doc = ranking.to_document()
        assert doc["week"] == "2026-02-01"
        assert doc["country"] == "united-states"
        assert doc["country_name"] == "United States"
        assert doc["category"] == "films"
        assert doc["source"] == "tsv"
        assert doc["fetched_at"] == now
        assert len(doc["rankings"]) == 1
        assert doc["rankings"][0]["title"] == "Movie A"

    def test_frozen(self):
        ranking = CountryRanking(
            week="2026-02-01",
            country="us",
            country_name="US",
            category="films",
            source="tsv",
            rankings=(),
        )
        try:
            ranking.week = "2026-01-01"
            assert False, "Should have raised FrozenInstanceError"
        except AttributeError:
            pass


class TestScrapeResult:
    def test_defaults(self):
        result = ScrapeResult(rankings=(), source_used="tsv")
        assert result.errors == ()

    def test_with_errors(self):
        result = ScrapeResult(
            rankings=(), source_used="none", errors=("err1", "err2")
        )
        assert len(result.errors) == 2


class TestScrapeRun:
    def test_to_document(self):
        now = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
        run = ScrapeRun(
            run_id="abc-123",
            started_at=now,
            completed_at=now,
            status="success",
            source_used="tsv",
            total_documents_saved=180,
            errors=("warning1",),
        )
        doc = run.to_document()
        assert doc["run_id"] == "abc-123"
        assert doc["status"] == "success"
        assert doc["total_documents_saved"] == 180
        assert doc["errors"] == ["warning1"]
