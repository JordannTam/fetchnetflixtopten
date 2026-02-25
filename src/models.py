"""Immutable data models for Netflix Top 10 rankings.

All dataclasses are frozen (immutable) to prevent accidental mutation.
Each model has a to_document() method that converts it to a dict
suitable for MongoDB insertion.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class RankingEntry:
    """A single show/film in a Top 10 list.

    Attributes:
        rank: Position 1-10 in the list.
        title: Show or film title.
        weeks_in_top_10: Cumulative weeks this title has appeared in the Top 10.
        hours_viewed: Total hours viewed (only available from TSV source, 0 otherwise).
    """

    rank: int
    title: str
    weeks_in_top_10: int
    hours_viewed: int = 0

    def to_document(self) -> dict:
        """Convert to a MongoDB-ready dict. Omits hours_viewed if zero."""
        doc = {
            "rank": self.rank,
            "title": self.title,
            "weeks_in_top_10": self.weeks_in_top_10,
        }
        if self.hours_viewed > 0:
            doc["hours_viewed"] = self.hours_viewed
        return doc


@dataclass(frozen=True)
class CountryRanking:
    """Top 10 rankings for one country, one category, one week.

    This is the core unit of data stored in MongoDB. One document per
    unique combination of (week, country, category).

    Attributes:
        week: ISO date string for the Netflix week (e.g. "2026-02-01").
        country: URL slug (e.g. "south-korea").
        country_name: Human-readable name (e.g. "South Korea").
        category: Either "films" or "tv".
        source: Where the data came from ("tsv" or "html_fallback").
        rankings: Tuple of 10 RankingEntry objects, sorted by rank.
        fetched_at: UTC timestamp when this data was collected.
    """

    week: str
    country: str
    country_name: str
    category: str
    source: str
    rankings: tuple[RankingEntry, ...]
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_document(self) -> dict:
        """Convert to a MongoDB-ready dict with nested ranking entries."""
        return {
            "week": self.week,
            "country": self.country,
            "country_name": self.country_name,
            "category": self.category,
            "fetched_at": self.fetched_at,
            "source": self.source,
            "rankings": [entry.to_document() for entry in self.rankings],
        }


@dataclass(frozen=True)
class ScrapeResult:
    """Result of a fetch operation from the orchestrator.

    Attributes:
        rankings: All collected CountryRanking objects.
        source_used: Which source succeeded ("tsv", "html_fallback", or "none").
        errors: Any errors encountered during fetching.
    """

    rankings: tuple[CountryRanking, ...]
    source_used: str
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class ScrapeRun:
    """Audit record for one Lambda invocation.

    Stored in the scrape_runs collection so every execution is traceable.

    Attributes:
        run_id: UUID identifying this run.
        started_at: UTC timestamp when the run began.
        completed_at: UTC timestamp when the run finished.
        status: One of "success", "partial_failure", or "failure".
        source_used: Which data source was used.
        total_documents_saved: Number of rankings upserted to MongoDB.
        errors: Any errors encountered during the run.
    """

    run_id: str
    started_at: datetime
    completed_at: datetime
    status: str
    source_used: str
    total_documents_saved: int
    errors: tuple[str, ...] = ()

    def to_document(self) -> dict:
        """Convert to a MongoDB-ready dict for the scrape_runs collection."""
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "status": self.status,
            "source_used": self.source_used,
            "total_documents_saved": self.total_documents_saved,
            "errors": list(self.errors),
        }
