"""HTTP session factory with automatic retries and exponential backoff.

Creates a requests.Session pre-configured with:
- Honest User-Agent header (not browser impersonation)
- Retry on transient HTTP errors (429, 5xx) with exponential backoff
- Configurable timeout and retry count from NetflixConfig
"""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import NetflixConfig


def create_session(config: NetflixConfig) -> requests.Session:
    """Create an HTTP session with retry strategy and honest User-Agent.

    Uses urllib3's Retry to automatically retry failed requests with
    exponential backoff (1s, 2s, 4s). Only retries on safe GET requests
    and only for transient server errors or rate limiting.

    Args:
        config: Netflix configuration with user_agent and retry_count.

    Returns:
        A requests.Session ready to use for all HTTP calls.
    """
    session = requests.Session()
    session.headers["User-Agent"] = config.user_agent

    retry_strategy = Retry(
        total=config.retry_count,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session
