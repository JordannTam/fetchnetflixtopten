"""Artist-linking pipeline for Netflix rankings.

Resolves ranking titles to canonical content IDs (TMDB), fetches credits,
matches only tracked artists from general.artists, and materializes weekly
artist drama scores in general.artist_drama_score.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from difflib import SequenceMatcher

import requests
from bson import ObjectId

from src.config import NetflixConfig
from src.models import ContentRef, CountryRanking
from src.storage.repository import RankingsRepository

logger = logging.getLogger(__name__)

_NORMALIZE_PATTERN = re.compile(r"[^\w]", flags=re.UNICODE)


@dataclass(frozen=True)
class LinkingMetrics:
    content_resolved: int = 0
    artists_linked: int = 0
    ambiguous_matches: int = 0
    unmatched_entries: int = 0
    drama_scores_upserted: int = 0


@dataclass(frozen=True)
class ArtistLinkingResult:
    rankings: tuple[CountryRanking, ...]
    content_docs: tuple[dict, ...]
    link_docs: tuple[dict, ...]
    review_docs: tuple[dict, ...]
    drama_score_docs: tuple[dict, ...]
    metrics: LinkingMetrics


def _normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "").casefold()
    return _NORMALIZE_PATTERN.sub("", normalized)


def _artist_key(artist: dict) -> ObjectId:
    # Artists are uniquely identified by the Mongo _id (ObjectId).
    # Caller must ensure the doc has _id (every Mongo doc does).
    return artist["_id"]


def _rank_score(rank: int) -> int:
    return max(11 - rank, 0)


def _dp_netflix(rank: int, weeks_in_top_10: int, hours_viewed: int, weeks_cap: int = 10) -> float:
    # Longevity bonus capped to avoid old lingering shows dominating new hits indefinitely.
    # e.g. a show at #10 for 50 weeks would otherwise outscore a new #1 by 490 pts.
    capped_weeks = min(weeks_in_top_10, weeks_cap)
    return _rank_score(rank) + (capped_weeks * 10) + (hours_viewed / 1_000_000)


def _iso_year_week(week: str) -> tuple[str, int]:
    try:
        parsed = datetime.strptime(week, "%Y-%m-%d")
    except ValueError:
        return "", 0
    iso_year, iso_week, _ = parsed.isocalendar()
    return str(iso_year), iso_week


def _tmdb_media_type(category: str) -> str:
    return "tv" if category == "tv" else "movie"


def _tmdb_search(
    session: requests.Session,
    config: NetflixConfig,
    title: str,
    category: str,
) -> dict | None:
    if not config.tmdb_api_key:
        return None

    media_type = _tmdb_media_type(category)
    response = session.get(
        f"{config.tmdb_base_url}/search/{media_type}",
        params={
            "api_key": config.tmdb_api_key,
            "query": title,
            "include_adult": "false",
        },
        timeout=config.tmdb_timeout,
    )
    response.raise_for_status()
    results = response.json().get("results", [])
    return results[0] if results else None


def _tmdb_credits(
    session: requests.Session,
    config: NetflixConfig,
    media_type: str,
    tmdb_id: int,
) -> tuple[dict, ...]:
    if not config.tmdb_api_key:
        return ()
    response = session.get(
        f"{config.tmdb_base_url}/{media_type}/{tmdb_id}/credits",
        params={"api_key": config.tmdb_api_key},
        timeout=config.tmdb_timeout,
    )
    response.raise_for_status()
    return tuple(response.json().get("cast", []))


def _index_artists(artists: tuple[dict, ...]) -> tuple[dict[str, list[dict]], dict[ObjectId, dict]]:
    by_name: dict[str, list[dict]] = {}
    by_id: dict[ObjectId, dict] = {}
    for artist in artists:
        if "_id" not in artist:
            continue
        artist_id = _artist_key(artist)
        by_id[artist_id] = artist
        aliases = artist.get("aliases") or ()
        normalized_aliases = artist.get("normalized_aliases") or ()
        names = [
            artist.get("english_name", ""),
            artist.get("korean_name", ""),
            *aliases,
            *normalized_aliases,
        ]
        for name in names:
            if not isinstance(name, str):
                continue
            normalized = _normalize_name(name)
            if not normalized:
                continue
            by_name.setdefault(normalized, []).append(artist)
    return by_name, by_id


def _best_fuzzy_match(
    normalized_credit: str,
    normalized_artist_names: dict[str, list[dict]],
) -> tuple[dict | None, float]:
    best_artist: dict | None = None
    best_score = 0.0
    credit_len = len(normalized_credit)
    for candidate_name, artists in normalized_artist_names.items():
        # Pre-filter: names differing by >50% length rarely score >= 0.85
        candidate_len = len(candidate_name)
        max_len = max(credit_len, candidate_len, 1)
        if abs(credit_len - candidate_len) / max_len > 0.5:
            continue
        score = SequenceMatcher(None, normalized_credit, candidate_name).ratio()
        if score > best_score:
            best_score = score
            best_artist = artists[0]
            if best_score == 1.0:
                break  # perfect match, no need to continue
    return best_artist, best_score


def _resolve_content_for_entry(
    entry: object,
    ranking: CountryRanking,
    session: requests.Session,
    config: NetflixConfig,
    search_cache: dict[tuple[str, str], dict | None],
    credits_cache: dict[str, tuple[dict, ...]],
    content_docs: dict[tuple[str, str], dict],
) -> tuple[ContentRef | None, tuple[dict, ...], bool]:
    """Search TMDB for entry title and fetch credits. Updates content_docs in place.

    Returns (content_ref, credits, tmdb_id_missing) where tmdb_id_missing=True
    means TMDB returned a result but it had no ID — the caller should skip the entry.
    """
    search_key = (ranking.category, entry.title)
    if search_key not in search_cache:
        try:
            search_cache[search_key] = _tmdb_search(session, config, entry.title, ranking.category)
        except (requests.RequestException, ValueError) as exc:
            logger.warning("TMDB search failed for %s: %s", entry.title, exc)
            search_cache[search_key] = None

    tmdb_content = search_cache[search_key]
    if not tmdb_content:
        return None, (), False

    tmdb_id = tmdb_content.get("id")
    if tmdb_id is None:
        return None, (), True  # signal: count as unmatched and skip entry

    provider_content_id = str(tmdb_id)
    media_type = _tmdb_media_type(ranking.category)
    content_ref = ContentRef(provider="tmdb", provider_content_id=provider_content_id)
    content_key = ("tmdb", provider_content_id)

    existing = content_docs.get(content_key)
    aliases: list[str] = list(existing.get("aliases", [])) if existing else []
    if entry.title not in aliases:
        aliases.append(entry.title)

    content_docs[content_key] = {
        "provider": "tmdb",
        "provider_content_id": provider_content_id,
        "title": tmdb_content.get("title") or tmdb_content.get("name") or entry.title,
        "original_title": (
            tmdb_content.get("original_title")
            or tmdb_content.get("original_name")
            or entry.title
        ),
        "media_type": media_type,
        "release_year": (
            (tmdb_content.get("release_date") or tmdb_content.get("first_air_date") or "")[:4]
        ),
        "aliases": aliases,
        "external_ids": {"tmdb_id": provider_content_id},
        "updated_at": datetime.now(timezone.utc),
    }

    credits_cache_key = f"{media_type}:{provider_content_id}"
    if credits_cache_key not in credits_cache:
        try:
            credits_cache[credits_cache_key] = _tmdb_credits(
                session, config, media_type, int(provider_content_id)
            )
        except (requests.RequestException, ValueError) as exc:
            logger.warning("TMDB credits failed for %s: %s", provider_content_id, exc)
            credits_cache[credits_cache_key] = ()

    return content_ref, credits_cache[credits_cache_key], False


def _process_exact_candidates(
    exact_candidates: list[dict],
    credit: dict,
    credit_name: str,
    ranking: CountryRanking,
    entry: object,
    provider_content_id: str,
    link_docs: dict[tuple[str, str, ObjectId, str], dict],
    review_docs: dict[tuple[str, str, str, str, str, ObjectId], dict],
) -> tuple[list[ObjectId], bool]:
    """Route exact-match candidates: auto-link singletons, queue ambiguous multiples.

    Returns (new_artist_ids, has_ambiguous).
    """
    candidates_by_tenant: dict[str, list[dict]] = {}
    for candidate in exact_candidates:
        tenant_id = str(candidate.get("tenant_id", ""))
        candidates_by_tenant.setdefault(tenant_id, []).append(candidate)

    new_artist_ids: list[ObjectId] = []
    has_ambiguous = False

    for tenant_id, tenant_candidates in candidates_by_tenant.items():
        if len(tenant_candidates) == 1:
            artist = tenant_candidates[0]
            artist_id = _artist_key(artist)
            new_artist_ids.append(artist_id)
            link_docs[(tenant_id, provider_content_id, artist_id, "cast")] = {
                "tenant_id": artist.get("tenant_id"),
                "content_provider": "tmdb",
                "content_id": provider_content_id,
                "artist_id": artist_id,
                "role": "cast",
                "character_name": credit.get("character"),
                "billing_order": credit.get("order"),
                "confidence": 1.0,
                "match_source": "exact_alias",
                "resolver_version": "v1",
                "updated_at": datetime.now(timezone.utc),
            }
        else:
            for candidate in tenant_candidates:
                candidate_artist_id = _artist_key(candidate)
                review_key = (
                    tenant_id,
                    ranking.week,
                    ranking.country,
                    ranking.category,
                    credit_name,
                    candidate_artist_id,
                )
                review_docs[review_key] = {
                    "tenant_id": candidate.get("tenant_id"),
                    "week": ranking.week,
                    "country": ranking.country,
                    "category": ranking.category,
                    "rank_title": entry.title,
                    "credit_name": credit_name,
                    "candidate_artist_id": candidate_artist_id,
                    "confidence": 0.99,
                    "status": "pending",
                    "created_at": datetime.now(timezone.utc),
                }
            has_ambiguous = True

    return new_artist_ids, has_ambiguous


def _match_credits_to_artists(
    credits: tuple[dict, ...],
    ranking: CountryRanking,
    entry: object,
    provider_content_id: str,
    normalized_artist_names: dict[str, list[dict]],
    config: NetflixConfig,
    link_docs: dict[tuple[str, str, ObjectId, str], dict],
    review_docs: dict[tuple[str, str, str, str, str, ObjectId], dict],
) -> tuple[list[ObjectId], bool, int]:
    """Match all credits for an entry against tracked artists.

    Returns (linked_artist_ids, has_ambiguous, ambiguous_count).
    """
    linked_artist_ids: list[ObjectId] = []
    has_ambiguous = False
    ambiguous_count = 0

    for credit in credits:
        credit_name = credit.get("name", "")
        normalized_credit = _normalize_name(credit_name)
        if not normalized_credit:
            continue

        exact_raw = normalized_artist_names.get(normalized_credit, [])
        seen_ids: set[ObjectId] = set()
        exact_candidates: list[dict] = []
        for candidate in exact_raw:
            cid = _artist_key(candidate)
            if cid not in seen_ids:
                seen_ids.add(cid)
                exact_candidates.append(candidate)

        if exact_candidates:
            new_ids, is_ambiguous = _process_exact_candidates(
                exact_candidates, credit, credit_name, ranking, entry,
                provider_content_id, link_docs, review_docs,
            )
            for aid in new_ids:
                if aid not in linked_artist_ids:
                    linked_artist_ids.append(aid)
            if is_ambiguous:
                has_ambiguous = True
                ambiguous_count += 1
            continue

        best_artist, fuzzy_score = _best_fuzzy_match(normalized_credit, normalized_artist_names)
        if best_artist and fuzzy_score >= config.fuzzy_review_threshold:
            tenant_id = str(best_artist.get("tenant_id", ""))
            candidate_artist_id = _artist_key(best_artist)
            review_key = (
                tenant_id,
                ranking.week,
                ranking.country,
                ranking.category,
                credit_name,
                candidate_artist_id,
            )
            review_docs[review_key] = {
                "tenant_id": best_artist.get("tenant_id"),
                "week": ranking.week,
                "country": ranking.country,
                "category": ranking.category,
                "rank_title": entry.title,
                "credit_name": credit_name,
                "candidate_artist_id": candidate_artist_id,
                "confidence": round(fuzzy_score, 4),
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
            }
            has_ambiguous = True
            ambiguous_count += 1

    return linked_artist_ids, has_ambiguous, ambiguous_count


def _accumulate_artist_scores(
    linked_artist_ids: list[ObjectId],
    entry: object,
    ranking: CountryRanking,
    content_ref: ContentRef,
    artists_by_id: dict[ObjectId, dict],
    title_score_max: dict[tuple[str, ObjectId, str, int, str, str], float],
    weeks_cap: int,
) -> bool:
    """Update title_score_max for each linked artist. Returns False if week is invalid."""
    year, week = _iso_year_week(ranking.week)
    if not year or week <= 0:
        logger.warning("Skipping score write for invalid week %s", ranking.week)
        return False

    for artist_id in linked_artist_ids:
        artist_doc = artists_by_id.get(artist_id)
        if not artist_doc:
            continue
        tenant_key = str(artist_doc.get("tenant_id", ""))
        title_key = (tenant_key, artist_id, year, week, "GLOBAL", content_ref.provider_content_id)
        contribution = _dp_netflix(entry.rank, entry.weeks_in_top_10, entry.hours_viewed, weeks_cap)
        if contribution > title_score_max.get(title_key, 0.0):
            title_score_max[title_key] = contribution

    return True


def link_rankings_to_artists(
    rankings: tuple[CountryRanking, ...],
    session: requests.Session,
    config: NetflixConfig,
    repo: RankingsRepository,
) -> ArtistLinkingResult:
    """Resolve content and match credits to tracked artists."""
    artists = repo.load_tracked_artists()
    normalized_artist_names, artists_by_id = _index_artists(artists)

    content_docs: dict[tuple[str, str], dict] = {}
    link_docs: dict[tuple[str, str, ObjectId, str], dict] = {}
    review_docs: dict[tuple[str, str, str, str, str, ObjectId], dict] = {}
    title_score_max: dict[tuple[str, ObjectId, str, int, str, str], float] = {}
    search_cache: dict[tuple[str, str], dict | None] = {}
    credits_cache: dict[str, tuple[dict, ...]] = {}

    content_resolved = 0
    artists_linked = 0
    ambiguous_matches = 0
    unmatched_entries = 0
    enriched_rankings: list[CountryRanking] = []

    for ranking in rankings:
        updated_entries = []
        for entry in ranking.rankings:
            content_ref, credits, tmdb_id_missing = _resolve_content_for_entry(
                entry, ranking, session, config, search_cache, credits_cache, content_docs,
            )

            if tmdb_id_missing:
                unmatched_entries += 1
                updated_entries.append(replace(entry))
                continue

            if content_ref:
                content_resolved += 1

            linked_artist_ids: list[str] = []
            has_ambiguous = False

            if content_ref and credits:
                linked_artist_ids, has_ambiguous, new_ambiguous = _match_credits_to_artists(
                    credits, ranking, entry, content_ref.provider_content_id,
                    normalized_artist_names, config, link_docs, review_docs,
                )
                ambiguous_matches += new_ambiguous

            if linked_artist_ids:
                match_status = "matched"
            elif content_ref and has_ambiguous:
                match_status = "ambiguous"
            else:
                match_status = "unmatched"
                unmatched_entries += 1

            if match_status == "matched":
                artists_linked += len(linked_artist_ids)
                valid_week = _accumulate_artist_scores(
                    linked_artist_ids, entry, ranking, content_ref,
                    artists_by_id, title_score_max, config.weeks_top10_cap,
                )
                if not valid_week:
                    updated_entries.append(
                        replace(
                            entry,
                            content_ref=content_ref,
                            match_status=match_status,
                            linked_artist_ids=tuple(linked_artist_ids),
                        )
                    )
                    continue

            updated_entries.append(
                replace(
                    entry,
                    content_ref=content_ref,
                    match_status=match_status,
                    linked_artist_ids=tuple(linked_artist_ids),
                )
            )

        enriched_rankings.append(replace(ranking, rankings=tuple(updated_entries)))

    score_accumulator: dict[tuple[str, ObjectId, str, int, str], float] = {}
    for title_key, contribution in title_score_max.items():
        tenant_key, artist_id, year, week, country, _ = title_key
        score_key = (tenant_key, artist_id, year, week, country)
        score_accumulator[score_key] = score_accumulator.get(score_key, 0.0) + contribution

    drama_score_docs = _build_drama_score_docs(score_accumulator, artists_by_id)
    metrics = LinkingMetrics(
        content_resolved=content_resolved,
        artists_linked=artists_linked,
        ambiguous_matches=ambiguous_matches,
        unmatched_entries=unmatched_entries,
        drama_scores_upserted=len(drama_score_docs),
    )
    return ArtistLinkingResult(
        rankings=tuple(enriched_rankings),
        content_docs=tuple(content_docs.values()),
        link_docs=tuple(link_docs.values()),
        review_docs=tuple(review_docs.values()),
        drama_score_docs=tuple(drama_score_docs),
        metrics=metrics,
    )


def _build_drama_score_docs(
    score_accumulator: dict[tuple[str, ObjectId, str, int, str], float],
    artists_by_id: dict[ObjectId, dict],
) -> list[dict]:
    by_scope: dict[
        tuple[str, str, int, str],
        list[tuple[tuple[str, ObjectId, str, int, str], float]],
    ] = {}
    for key, score in score_accumulator.items():
        tenant_id, _, year, week, country = key
        by_scope.setdefault((tenant_id, year, week, country), []).append((key, score))

    docs: list[dict] = []
    now = datetime.now(timezone.utc)
    for values in by_scope.values():
        max_score = max(score for _, score in values)
        for key, score in values:
            _, artist_id, year, week, country = key
            artist = artists_by_id.get(artist_id)
            if not artist:
                logger.warning(
                    "Skipping drama score doc: artist_id %s not in index", artist_id
                )
                continue
            # Ratio normalization: score as % of the top performer in this tenant-week scope.
            # Preserves absolute meaning — 0 = no contribution, 100 = best this week.
            # Min-max was discarded because it maps the lowest scorer to 0.0 regardless
            # of absolute performance, which is misleading with small group sizes.
            normalized = 100.0 if max_score == 0 else (score / max_score) * 100
            docs.append(
                {
                    "tenant_id": artist.get("tenant_id") or None,
                    "artist_id": _artist_key(artist),
                    "english_name": artist.get("english_name", ""),
                    "korean_name": artist.get("korean_name", ""),
                    "type": artist.get("type", []),
                    "image": artist.get("image", artist.get("image_url", "")),
                    "year": year,
                    "week": week,
                    "country": country,
                    "netflix_score": round(score, 4),
                    "netflix_normalized": round(normalized, 4),
                    "drama_score": round(score, 4),
                    "updated_at": now,
                }
            )
    return docs
