# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import time
from typing import Any

import requests  # type: ignore[import-untyped]
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter  # type: ignore[import-untyped]
from urllib3.util.retry import Retry

from coreason_etl_pmda.config import settings
from coreason_etl_pmda.utils_logger import logger


def get_session() -> requests.Session:
    """
    Returns a configured requests Session with retry logic.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": settings.USER_AGENT})

    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


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
    Fetches a URL with rate limiting, error handling, and encoding detection.

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
    # Mandatory Rate Limit
    time.sleep(settings.SCRAPING_RATE_LIMIT_DELAY)

    if session is None:
        # If no session provided, use a one-off session (with retries)
        session = get_session()

    try:
        logger.debug(f"Fetching {url} [{method}]")
        response = session.request(
            method=method,
            url=url,
            params=params,
            data=data,
            json=json,
            timeout=timeout,
        )
        response.raise_for_status()

        # Robust Encoding Detection
        # PMDA often uses Shift-JIS / CP932.
        # If headers don't specify charset, requests might default to ISO-8859-1.
        # We try to detect using BeautifulSoup or fallback to standard Japanese encodings.

        # If encoding is not explicitly in Content-Type, we might need to guess.
        # We check `response.encoding`. If it's ISO-8859-1, it's likely wrong for Japanese sites.
        if response.encoding and response.encoding.lower() == "iso-8859-1":
            # Try to peek into content using BS4 (which uses chardet/charset-normalizer under the hood)
            # or just default to apparent_encoding
            response.encoding = response.apparent_encoding

        # Further check: sometimes apparent_encoding is not perfect (e.g. EUC-JP vs Shift_JIS).
        # We allow the caller to handle specific decoding if needed, but we try our best here.
        # BS4 is usually good at guessing when parsing HTML.
        # For non-HTML (like CSV/Zip), the caller handles binary usually.

        return response

    except requests.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        raise


def get_soup(response: requests.Response) -> BeautifulSoup:
    """
    Helper to get BeautifulSoup object from response, handling encoding.
    """
    # We pass the content bytes to BS4 so it can detect encoding from meta tags if available.
    return BeautifulSoup(response.content, "html.parser")
