"""Data validation for scraped rankings.

Validates the integrity of data after fetching, before storing to
MongoDB. Catches issues like:
- Ranks outside 1-10 (indicates parsing error)
- Empty titles (indicates selector broke)
- Duplicate ranks (indicates data corruption)
- Wrong number of entries (indicates partial page load)

Validation errors prevent storage. Warnings are logged but don't
block the pipeline - they indicate unusual but not necessarily
broken data (e.g. a country with only 8 entries instead of 10).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.models import CountryRanking

logger = logging.getLogger(__name__)

MAX_RANK = 10
MIN_RANK = 1


@dataclass(frozen=True)
class ValidationResult:
    """Result of validating a single CountryRanking.

    Attributes:
        valid: True if no errors (warnings are OK).
        errors: Issues that should prevent storage.
        warnings: Unusual data that's still acceptable.
    """

    valid: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]


def validate_ranking(ranking: CountryRanking) -> ValidationResult:
    """Validate a single country-category ranking.

    Checks:
    - Rankings tuple is non-empty
    - Category is "films" or "tv" (warns otherwise)
    - Week is not "unknown" (warns otherwise)
    - Each rank is between 1 and 10
    - Each title is non-empty
    - No duplicate ranks
    - Negative weeks_in_top_10 (warns)
    - Exactly 10 entries (warns if different)

    Args:
        ranking: A CountryRanking object to validate.

    Returns:
        ValidationResult with valid=True if no errors found.
    """
    errors: list[str] = []
    warnings: list[str] = []
    context = f"{ranking.country}/{ranking.category}/week={ranking.week}"

    if not ranking.rankings:
        errors.append(f"{context}: no ranking entries")
        return ValidationResult(
            valid=False,
            errors=tuple(errors),
            warnings=tuple(warnings),
        )

    if ranking.category not in ("films", "tv"):
        warnings.append(
            f"{context}: unexpected category '{ranking.category}'"
        )

    if ranking.week == "unknown":
        warnings.append(f"{context}: week is unknown")

    seen_ranks: set[int] = set()
    for entry in ranking.rankings:
        entry_ctx = f"{context}/rank={entry.rank}"

        if entry.rank < MIN_RANK or entry.rank > MAX_RANK:
            errors.append(
                f"{entry_ctx}: rank out of range [{MIN_RANK}-{MAX_RANK}]"
            )

        if not entry.title or not entry.title.strip():
            errors.append(f"{entry_ctx}: empty title")

        if entry.rank in seen_ranks:
            errors.append(f"{entry_ctx}: duplicate rank")
        seen_ranks.add(entry.rank)

        if entry.weeks_in_top_10 < 0:
            warnings.append(
                f"{entry_ctx}: negative weeks_in_top_10"
            )

    if len(ranking.rankings) != MAX_RANK:
        warnings.append(
            f"{context}: expected {MAX_RANK} entries, got "
            f"{len(ranking.rankings)}"
        )

    return ValidationResult(
        valid=len(errors) == 0,
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


def validate_all(
    rankings: tuple[CountryRanking, ...],
) -> tuple[ValidationResult, ...]:
    """Validate all rankings and log summary statistics.

    Args:
        rankings: Tuple of CountryRanking objects to validate.

    Returns:
        Tuple of ValidationResult objects, one per input ranking,
        in the same order.
    """
    results = []
    total_errors = 0
    total_warnings = 0

    for ranking in rankings:
        result = validate_ranking(ranking)
        results.append(result)
        total_errors += len(result.errors)
        total_warnings += len(result.warnings)

    if total_errors:
        logger.warning("Validation found %d errors", total_errors)
    if total_warnings:
        logger.info("Validation found %d warnings", total_warnings)

    return tuple(results)
