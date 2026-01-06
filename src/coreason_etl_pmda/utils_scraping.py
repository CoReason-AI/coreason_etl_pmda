# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import logging
from typing import Any

import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
    before_sleep_log,
)
from ratelimit import limits, sleep_and_retry

from coreason_etl_pmda.config import settings
from coreason_etl_pmda.utils_logger import logger


def get_session() -> requests.Session:
    """
    Returns a configured requests Session.
    Note: Retries are now handled by tenacity in fetch_url,
    so we don't need HTTPAdapter retries here anymore,
    but we keep the user agent.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": settings.USER_AGENT})
    return session


# Rate limit: 1 call per X seconds (defined in settings)
# We use sleep_and_retry to ensure the thread sleeps if limit is hit.
@sleep_and_retry  # type: ignore
@limits(calls=1, period=settings.SCRAPING_RATE_LIMIT_DELAY)  # type: ignore
def _rate_limited_request(
    session: requests.Session,
    method: str,
    url: str,
    **kwargs: Any,
) -> requests.Response:
    """
    Internal helper to execute the request with rate limiting.
    """
    return session.request(method=method, url=url, **kwargs)


def _should_retry_error(exception: BaseException) -> bool:
    """
    Custom retry predicate.
    Retries on:
    - Connection errors / Timeouts
    - ChunkedEncodingError
    - HTTPError only if status code is in [429, 500, 502, 503, 504]
    """
    # print(f"Predicate called for {type(exception)}: {exception}")
    if isinstance(
        exception,
        (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True

    if isinstance(exception, requests.HTTPError):
        # Check status code
        response = exception.response
        if response is not None:
            # print(f"Checking status: {response.status_code}")
            return response.status_code in [429, 500, 502, 503, 504]

    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(_should_retry_error),
    before_sleep=before_sleep_log(logging.getLogger("coreason_etl_pmda"), logging.WARNING),
    reraise=True,
)
def fetch_url(
    url: str,
    session: requests.Session | None = None,
    method: str = "GET",
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    json: Any | None = None,
    timeout: int = settings.SCRAPING_REQUEST_TIMEOUT,
) -> requests.Response:
    """
    Fetches a URL with rate limiting (via ratelimit), retries (via tenacity), and encoding detection.

    Args:
        url: The URL to fetch.
        session: Optional existing session. If None, a temporary one is created.
        method: HTTP method (GET, POST, etc.)
        params: Query parameters.
        data: Form data.
        json: JSON body.
        timeout: Request timeout in seconds.

    Returns:
        The response object.
    """
    if session is None:
        session = get_session()

    logger.debug(f"Fetching {url} [{method}]")

    try:
        # Pass request through rate limiter
        response = _rate_limited_request(
            session,
            method,
            url,
            params=params,
            data=data,
            json=json,
            timeout=timeout,
        )

        # Raise for status to trigger retry logic via exception
        response.raise_for_status()

        # Robust Encoding Detection
        if response.encoding and response.encoding.lower() == "iso-8859-1":
            response.encoding = response.apparent_encoding

        return response  # type: ignore[no-any-return]

    except Exception:
        # Let tenacity handle it (or bubble up if not retriable)
        raise


def get_soup(response: requests.Response) -> BeautifulSoup:
    """
    Helper to get BeautifulSoup object from response, handling encoding.
    """
    return BeautifulSoup(response.content, "html.parser")
