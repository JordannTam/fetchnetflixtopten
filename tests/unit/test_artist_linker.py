from unittest.mock import MagicMock

from bson import ObjectId

from src.config import NetflixConfig
from src.linking.artist_linker import _build_drama_score_docs, link_rankings_to_artists
from src.models import CountryRanking, RankingEntry


def _make_response(payload: dict):
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_link_rankings_matches_artist_by_alias():
    oid = ObjectId()
    ranking = CountryRanking(
        week="2026-02-01",
        country="south-korea",
        country_name="South Korea",
        category="tv",
        source="tsv",
        rankings=(
            RankingEntry(
                rank=1,
                title="Sample Drama",
                weeks_in_top_10=2,
                hours_viewed=2_000_000,
            ),
        ),
    )
    session = MagicMock()
    session.get.side_effect = [
        _make_response({"results": [{"id": 123, "name": "Sample Drama"}]}),
        _make_response({"cast": [{"name": "T-024", "order": 0, "character": "Self"}]}),
    ]

    config = NetflixConfig(tmdb_api_key="test_key")
    repo = MagicMock()
    repo.load_tracked_artists.return_value = (
        {
            "_id": oid,
            "english_name": "t024",
            "korean_name": "티공이사",
            "aliases": ["T-024"],
            "type": ["artist"],
            "image": "",
            "tenant_id": "tenant-1",
        },
    )

    result = link_rankings_to_artists((ranking,), session, config, repo)

    enriched = result.rankings[0].rankings[0]
    assert enriched.match_status == "matched"
    assert enriched.content_ref is not None
    assert oid in enriched.linked_artist_ids
    assert len(result.content_docs) == 1
    assert len(result.link_docs) == 1
    assert len(result.drama_score_docs) == 1


def test_link_rankings_without_tmdb_key_stays_unmatched():
    ranking = CountryRanking(
        week="2026-02-01",
        country="south-korea",
        country_name="South Korea",
        category="tv",
        source="tsv",
        rankings=(RankingEntry(rank=1, title="No Resolve", weeks_in_top_10=1),),
    )
    session = MagicMock()
    repo = MagicMock()
    repo.load_tracked_artists.return_value = ()

    result = link_rankings_to_artists((ranking,), session, NetflixConfig(), repo)

    enriched = result.rankings[0].rankings[0]
    assert enriched.match_status == "unmatched"
    assert result.content_docs == ()
    assert result.link_docs == ()


def test_exact_alias_collision_goes_to_review_queue():
    ranking = CountryRanking(
        week="2026-02-01",
        country="south-korea",
        country_name="South Korea",
        category="tv",
        source="tsv",
        rankings=(RankingEntry(rank=1, title="Sample Drama", weeks_in_top_10=1),),
    )
    session = MagicMock()
    session.get.side_effect = [
        _make_response({"results": [{"id": 123, "name": "Sample Drama"}]}),
        _make_response({"cast": [{"name": "Shared Alias"}]}),
    ]
    repo = MagicMock()
    repo.load_tracked_artists.return_value = (
        {"_id": ObjectId(), "aliases": ["Shared Alias"], "tenant_id": "t1"},
        {"_id": ObjectId(), "aliases": ["Shared Alias"], "tenant_id": "t1"},
    )

    result = link_rankings_to_artists(
        (ranking,), session, NetflixConfig(tmdb_api_key="x"), repo
    )
    entry = result.rankings[0].rankings[0]
    assert entry.match_status == "ambiguous"
    assert entry.linked_artist_ids == ()
    assert len(result.review_docs) == 2


def test_drama_score_normalization_is_tenant_scoped():
    oid_a = ObjectId()
    oid_b = ObjectId()
    docs = _build_drama_score_docs(
        {
            ("tenant-a", oid_a, "2026", 1, "GLOBAL"): 10.0,
            ("tenant-b", oid_b, "2026", 1, "GLOBAL"): 40.0,
        },
        {
            oid_a: {"_id": oid_a, "tenant_id": "tenant-a"},
            oid_b: {"_id": oid_b, "tenant_id": "tenant-b"},
        },
    )
    normalized = {doc["artist_id"]: doc["netflix_normalized"] for doc in docs}
    assert normalized[oid_a] == 100.0
    assert normalized[oid_b] == 100.0


def test_exact_alias_across_tenants_auto_links_each_tenant():
    ranking = CountryRanking(
        week="2026-02-01",
        country="south-korea",
        country_name="South Korea",
        category="tv",
        source="tsv",
        rankings=(RankingEntry(rank=1, title="Sample Drama", weeks_in_top_10=1),),
    )
    session = MagicMock()
    session.get.side_effect = [
        _make_response({"results": [{"id": 123, "name": "Sample Drama"}]}),
        _make_response({"cast": [{"name": "JISOO"}]}),
    ]
    oid_a = ObjectId()
    oid_b = ObjectId()
    repo = MagicMock()
    repo.load_tracked_artists.return_value = (
        {"_id": oid_a, "english_name": "Jisoo", "tenant_id": "tenant-a"},
        {"_id": oid_b, "english_name": "Jisoo", "tenant_id": "tenant-b"},
    )

    result = link_rankings_to_artists(
        (ranking,), session, NetflixConfig(tmdb_api_key="x"), repo
    )
    entry = result.rankings[0].rankings[0]
    assert entry.match_status == "matched"
    assert set(entry.linked_artist_ids) == {oid_a, oid_b}
    assert result.review_docs == ()


def test_artist_without_id_is_skipped():
    """Artists missing _id are filtered out (defensive guard)."""
    ranking = CountryRanking(
        week="2026-02-01",
        country="south-korea",
        country_name="South Korea",
        category="tv",
        source="tsv",
        rankings=(RankingEntry(rank=1, title="Sample Drama", weeks_in_top_10=1),),
    )
    session = MagicMock()
    session.get.side_effect = [
        _make_response({"results": [{"id": 123, "name": "Sample Drama"}]}),
        _make_response({"cast": [{"name": "Hyun Bin"}]}),
    ]
    repo = MagicMock()
    repo.load_tracked_artists.return_value = (
        {"english_name": "Hyun Bin", "tenant_id": "tenant-1"},  # no _id
    )

    result = link_rankings_to_artists(
        (ranking,), session, NetflixConfig(tmdb_api_key="x"), repo
    )
    entry = result.rankings[0].rankings[0]
    assert entry.match_status == "unmatched"
    assert entry.linked_artist_ids == ()


def test_global_score_dedupes_same_content_across_countries():
    rankings = (
        CountryRanking(
            week="2026-02-01",
            country="south-korea",
            country_name="South Korea",
            category="tv",
            source="tsv",
            rankings=(RankingEntry(rank=1, title="Shared Drama", weeks_in_top_10=1),),
        ),
        CountryRanking(
            week="2026-02-01",
            country="japan",
            country_name="Japan",
            category="tv",
            source="tsv",
            rankings=(RankingEntry(rank=10, title="Shared Drama", weeks_in_top_10=1),),
        ),
    )
    session = MagicMock()
    session.get.side_effect = [
        _make_response({"results": [{"id": 555, "name": "Shared Drama"}]}),
        _make_response({"cast": [{"name": "Actor A"}]}),
    ]
    repo = MagicMock()
    repo.load_tracked_artists.return_value = (
        {"_id": ObjectId(), "english_name": "Actor A", "tenant_id": "tenant-a"},
    )

    result = link_rankings_to_artists(
        rankings, session, NetflixConfig(tmdb_api_key="x"), repo
    )
    assert len(result.drama_score_docs) == 1
    score = result.drama_score_docs[0]["netflix_score"]
    # rank 1 contribution (10 + 10) should win over rank 10 (1 + 10).
    assert score == 20.0
