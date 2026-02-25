from src.models import CountryRanking, RankingEntry
from src.validation.validators import validate_all, validate_ranking


def _make_ranking(entries=None, category="films", week="2026-02-01"):
    if entries is None:
        entries = tuple(
            RankingEntry(rank=i, title=f"Title {i}", weeks_in_top_10=1)
            for i in range(1, 11)
        )
    return CountryRanking(
        week=week,
        country="united-states",
        country_name="United States",
        category=category,
        source="tsv",
        rankings=entries,
    )


class TestValidateRanking:
    def test_valid_ranking(self):
        ranking = _make_ranking()
        result = validate_ranking(ranking)
        assert result.valid is True
        assert result.errors == ()

    def test_empty_rankings(self):
        ranking = _make_ranking(entries=())
        result = validate_ranking(ranking)
        assert result.valid is False
        assert any("no ranking entries" in e for e in result.errors)

    def test_rank_out_of_range(self):
        entries = (
            RankingEntry(rank=0, title="Bad Rank", weeks_in_top_10=1),
        )
        ranking = _make_ranking(entries=entries)
        result = validate_ranking(ranking)
        assert result.valid is False
        assert any("out of range" in e for e in result.errors)

    def test_rank_above_max(self):
        entries = (
            RankingEntry(rank=11, title="Too High", weeks_in_top_10=1),
        )
        ranking = _make_ranking(entries=entries)
        result = validate_ranking(ranking)
        assert result.valid is False

    def test_empty_title(self):
        entries = (
            RankingEntry(rank=1, title="", weeks_in_top_10=1),
        )
        ranking = _make_ranking(entries=entries)
        result = validate_ranking(ranking)
        assert result.valid is False
        assert any("empty title" in e for e in result.errors)

    def test_duplicate_ranks(self):
        entries = (
            RankingEntry(rank=1, title="A", weeks_in_top_10=1),
            RankingEntry(rank=1, title="B", weeks_in_top_10=1),
        )
        ranking = _make_ranking(entries=entries)
        result = validate_ranking(ranking)
        assert result.valid is False
        assert any("duplicate rank" in e for e in result.errors)

    def test_unexpected_category_warning(self):
        ranking = _make_ranking(category="documentaries")
        result = validate_ranking(ranking)
        assert result.valid is True
        assert any("unexpected category" in w for w in result.warnings)

    def test_unknown_week_warning(self):
        ranking = _make_ranking(week="unknown")
        result = validate_ranking(ranking)
        assert result.valid is True
        assert any("week is unknown" in w for w in result.warnings)

    def test_wrong_entry_count_warning(self):
        entries = tuple(
            RankingEntry(rank=i, title=f"T{i}", weeks_in_top_10=1)
            for i in range(1, 6)
        )
        ranking = _make_ranking(entries=entries)
        result = validate_ranking(ranking)
        assert result.valid is True
        assert any("expected 10" in w for w in result.warnings)


class TestValidateAll:
    def test_multiple_rankings(self):
        r1 = _make_ranking()
        r2 = _make_ranking(category="tv")
        results = validate_all((r1, r2))
        assert len(results) == 2
        assert all(r.valid for r in results)

    def test_empty_input(self):
        results = validate_all(())
        assert results == ()
