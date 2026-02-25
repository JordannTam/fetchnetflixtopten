import os

from src.fetchers.html_fetcher import _extract_rankings, _extract_week
from bs4 import BeautifulSoup

FIXTURES_DIR = os.path.join(
    os.path.dirname(__file__), "..", "fixtures"
)


def _load_soup() -> BeautifulSoup:
    path = os.path.join(FIXTURES_DIR, "sample_top10.html")
    with open(path, "r") as f:
        return BeautifulSoup(f.read(), "lxml")


class TestExtractWeek:
    def test_extracts_week_from_fixture(self):
        soup = _load_soup()
        week = _extract_week(soup)
        assert week != "unknown"
        assert "2026" in week or "2025" in week

    def test_returns_unknown_for_empty_soup(self):
        soup = BeautifulSoup("<html></html>", "lxml")
        week = _extract_week(soup)
        assert week == "unknown"


class TestExtractRankings:
    def test_extracts_10_entries(self):
        soup = _load_soup()
        entries = _extract_rankings(soup)
        assert len(entries) == 10

    def test_ranks_sequential(self):
        soup = _load_soup()
        entries = _extract_rankings(soup)
        ranks = [e.rank for e in entries]
        assert ranks == list(range(1, 11))

    def test_titles_non_empty(self):
        soup = _load_soup()
        entries = _extract_rankings(soup)
        for entry in entries:
            assert entry.title
            assert len(entry.title) > 0

    def test_weeks_in_top_10_present(self):
        soup = _load_soup()
        entries = _extract_rankings(soup)
        for entry in entries:
            assert entry.weeks_in_top_10 >= 0

    def test_empty_html(self):
        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        entries = _extract_rankings(soup)
        assert entries == ()

    def test_source_is_html_fallback(self):
        soup = _load_soup()
        entries = _extract_rankings(soup)
        assert all(e.hours_viewed == 0 for e in entries)
