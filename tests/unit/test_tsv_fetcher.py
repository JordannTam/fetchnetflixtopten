import os

import responses

from src.config import NetflixConfig
from src.fetchers.http_client import create_session
from src.fetchers.tsv_fetcher import (
    COUNTRIES_TSV_URL,
    _country_slug,
    _parse_countries_tsv,
    _parse_int,
    fetch_latest_week,
)

FIXTURES_DIR = os.path.join(
    os.path.dirname(__file__), "..", "fixtures"
)


def _read_fixture(filename: str) -> str:
    path = os.path.join(FIXTURES_DIR, filename)
    with open(path, "r") as f:
        return f.read()


class TestHelpers:
    def test_country_slug(self):
        assert _country_slug("United States") == "united-states"
        assert _country_slug("South Korea") == "south-korea"
        assert _country_slug("Brazil") == "brazil"

    def test_parse_int_valid(self):
        assert _parse_int("42") == 42

    def test_parse_int_invalid(self):
        assert _parse_int("abc") == 0
        assert _parse_int("") == 0
        assert _parse_int(None) == 0


class TestParseTsv:
    def test_parse_fixture(self):
        tsv_text = _read_fixture("sample_countries.tsv")
        rankings = _parse_countries_tsv(tsv_text)
        assert len(rankings) > 0

        countries = {r.country_name for r in rankings}
        assert "United States" in countries
        assert "South Korea" in countries

        categories = {r.category for r in rankings}
        assert "films" in categories
        assert "tv" in categories

    def test_all_entries_have_valid_data(self):
        tsv_text = _read_fixture("sample_countries.tsv")
        rankings = _parse_countries_tsv(tsv_text)
        for ranking in rankings:
            assert ranking.week == "2026-02-01"
            assert ranking.source == "tsv"
            assert len(ranking.rankings) == 10
            for entry in ranking.rankings:
                assert 1 <= entry.rank <= 10
                assert entry.title
                assert entry.weeks_in_top_10 >= 0

    def test_parse_specific_week(self):
        tsv_text = _read_fixture("sample_countries.tsv")
        rankings = _parse_countries_tsv(
            tsv_text, target_week="2026-02-01"
        )
        assert all(r.week == "2026-02-01" for r in rankings)

    def test_parse_nonexistent_week(self):
        tsv_text = _read_fixture("sample_countries.tsv")
        rankings = _parse_countries_tsv(
            tsv_text, target_week="1999-01-01"
        )
        assert rankings == ()

    def test_rankings_sorted_by_rank(self):
        tsv_text = _read_fixture("sample_countries.tsv")
        rankings = _parse_countries_tsv(tsv_text)
        for ranking in rankings:
            ranks = [e.rank for e in ranking.rankings]
            assert ranks == sorted(ranks)


class TestFetchLatestWeek:
    @responses.activate
    def test_fetch_success(self):
        tsv_text = _read_fixture("sample_countries.tsv")
        responses.add(
            responses.GET,
            COUNTRIES_TSV_URL,
            body=tsv_text,
            status=200,
        )
        config = NetflixConfig()
        session = create_session(config)
        rankings = fetch_latest_week(session, config)
        assert len(rankings) == 4  # 2 countries x 2 categories

    @responses.activate
    def test_fetch_http_error(self):
        responses.add(
            responses.GET,
            COUNTRIES_TSV_URL,
            status=500,
        )
        config = NetflixConfig()
        session = create_session(config)
        try:
            fetch_latest_week(session, config)
            assert False, "Should have raised"
        except Exception:
            pass
