"""AWS Lambda entry point for the Netflix Top 10 scraper.

This is the thin orchestration layer that ties everything together.
It runs once per invocation (weekly via EventBridge) and:

    1. Loads config from environment variables
    2. Creates an HTTP session with retry/backoff
    3. Fetches rankings (TSV primary, HTML fallback)
    4. Validates the fetched data
    5. Stores valid rankings in MongoDB (upsert)
    6. Logs an audit record of the run

All logging is structured JSON for CloudWatch readability.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from pymongo.errors import PyMongoError
from requests.exceptions import RequestException

from src.config import MongoConfig, NetflixConfig, load_config
from src.fetchers import fetch_rankings
from src.fetchers.http_client import create_session
from src.models import ScrapeRun
from src.storage.mongo_client import get_database
from src.storage.repository import RankingsRepository
from src.validation.validators import validate_all

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class _JSONFormatter(logging.Formatter):
    """Format log records as single-line JSON for CloudWatch."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize the log record to a JSON string."""
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        })


_handler = logging.StreamHandler()
_handler.setFormatter(_JSONFormatter())
if not logger.handlers:
    logger.addHandler(_handler)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """AWS Lambda handler - entry point for weekly scrape job.

    Called by EventBridge on a weekly cron schedule, or manually via
    the Lambda console "Test" button. The event and context parameters
    are not used but required by the Lambda interface.

    Pipeline: load config -> fetch (TSV/HTML) -> validate -> store -> audit log

    Returns:
        Dict with statusCode (200 or 500) and JSON body containing
        run_id, status, source_used, saved count, and any errors.
    """
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    errors: list[str] = []

    logger.info("Starting scrape run %s", run_id)

    try:
        netflix_config, mongo_config = load_config()
    except ValueError as exc:
        logger.error("Configuration error: %s", exc)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(exc)}),
        }

    session = create_session(netflix_config)

    try:
        result = fetch_rankings(session, netflix_config)
    except (RequestException, ValueError, TimeoutError) as exc:
        logger.error("Fetch failed: %s", exc)
        errors.append(f"Fetch failed: {exc}")
        return _finish_run(
            run_id, started_at, "failure", "none", 0, errors,
            mongo_config,
        )

    if not result.rankings:
        errors.extend(result.errors)
        return _finish_run(
            run_id, started_at, "failure", result.source_used, 0, errors,
            mongo_config,
        )

    validation_results = validate_all(result.rankings)
    validation_errors = [
        err
        for vr in validation_results
        for err in vr.errors
    ]
    if validation_errors:
        errors.extend(validation_errors)
        logger.warning(
            "Validation produced %d errors", len(validation_errors)
        )

    try:
        db = get_database(mongo_config)
        repo = RankingsRepository(db, mongo_config)
        repo.ensure_indexes()
        saved = repo.save_rankings(result.rankings)
    except PyMongoError as exc:
        logger.error("Storage failed: %s", exc)
        errors.append(f"Storage failed: {exc}")
        return _finish_run(
            run_id, started_at, "failure", result.source_used, 0, errors,
            mongo_config,
        )

    status = "partial_failure" if errors else "success"
    return _finish_run(
        run_id, started_at, status, result.source_used, saved, errors,
        mongo_config,
    )


def _finish_run(
    run_id: str,
    started_at: datetime,
    status: str,
    source_used: str,
    saved: int,
    errors: list[str],
    mongo_config: MongoConfig,
) -> dict[str, Any]:
    """Record the scrape run to MongoDB and return the Lambda response.

    Called at every exit point (success, partial failure, or failure)
    to ensure the audit trail is always written, even when the main
    pipeline fails. If the audit write itself fails, it's logged but
    doesn't change the response.

    Args:
        run_id: UUID for this invocation.
        started_at: When the Lambda started executing.
        status: "success", "partial_failure", or "failure".
        source_used: Which data source was used ("tsv", "html_fallback", "none").
        saved: Number of documents upserted to MongoDB.
        errors: Accumulated error messages from the pipeline.
        mongo_config: MongoDB config for writing the audit record.

    Returns:
        Lambda response dict with statusCode and JSON body.
    """
    completed_at = datetime.now(timezone.utc)

    run = ScrapeRun(
        run_id=run_id,
        started_at=started_at,
        completed_at=completed_at,
        status=status,
        source_used=source_used,
        total_documents_saved=saved,
        errors=tuple(errors),
    )

    try:
        db = get_database(mongo_config)
        repo = RankingsRepository(db, mongo_config)
        repo.save_scrape_run(run)
    except PyMongoError as exc:
        logger.error("Failed to save scrape run: %s", exc)

    logger.info(
        "Run %s completed: status=%s, saved=%d, errors=%d",
        run_id, status, saved, len(errors),
    )

    return {
        "statusCode": 200 if status != "failure" else 500,
        "body": json.dumps({
            "run_id": run_id,
            "status": status,
            "source_used": source_used,
            "saved": saved,
            "errors": list(errors),
        }),
    }
