"""MongoDB connection management for AWS Lambda.

Uses a module-level singleton pattern for the MongoClient because
Lambda reuses the execution environment between invocations. This
means the TCP connection to MongoDB stays warm across invocations,
avoiding the ~1-2 second cold-start penalty of establishing a new
TLS connection each time.

maxPoolSize=1 because Lambda runs one invocation at a time per
container - there's no concurrency within a single Lambda instance.
"""

from __future__ import annotations

import logging

from pymongo import MongoClient
from pymongo.database import Database

from src.config import MongoConfig

logger = logging.getLogger(__name__)

_client: MongoClient | None = None


def get_database(config: MongoConfig) -> Database:
    """Get a MongoDB database handle, creating the connection if needed.

    On the first call, creates a new MongoClient with the configured
    URI and pool settings. On subsequent calls within the same Lambda
    container, returns the existing connection (warm start).

    Args:
        config: MongoDB configuration with URI and pool settings.

    Returns:
        A pymongo Database object for the configured database name.
    """
    global _client
    if _client is None:
        logger.info("Creating new MongoDB connection")
        _client = MongoClient(
            config.uri,
            maxPoolSize=config.max_pool_size,
            serverSelectionTimeoutMS=5000,
        )
    return _client[config.database]


def close_connection() -> None:
    """Close the MongoDB connection and reset the singleton.

    Should be called during Lambda shutdown or when explicitly
    cleaning up resources. Safe to call even if no connection exists.
    """
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("MongoDB connection closed")
