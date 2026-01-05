# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from unittest.mock import MagicMock, patch

import pytest
import requests
from bs4 import BeautifulSoup

from coreason_etl_pmda.utils_scraping import fetch_url, get_session, get_soup


def test_get_session() -> None:
    session = get_session()
    assert isinstance(session, requests.Session)
    assert "User-Agent" in session.headers
    assert session.headers["User-Agent"] == "CoReasonETL/1.0"

    # Check adapter mounting (checking keys in adapters)
    assert "http://" in session.adapters
    assert "https://" in session.adapters


def test_fetch_url_success() -> None:
    with patch("coreason_etl_pmda.utils_scraping.requests.Session.request") as mock_request:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.encoding = "utf-8"
        mock_request.return_value = mock_resp

        resp = fetch_url("http://example.com")
        assert resp == mock_resp
        mock_request.assert_called_once()
        # Verify rate limit sleep was called?
        # We can patch time.sleep to verify


def test_fetch_url_rate_limit() -> None:
    with patch("coreason_etl_pmda.utils_scraping.time.sleep") as mock_sleep:
        with patch("coreason_etl_pmda.utils_scraping.requests.Session.request") as mock_request:
            mock_resp = MagicMock()
            mock_request.return_value = mock_resp

            fetch_url("http://example.com")

            mock_sleep.assert_called_with(1.0)  # Default setting


def test_fetch_url_iso_8859_1_fix() -> None:
    """Test that we attempt to fix ISO-8859-1 encoding."""
    with patch("coreason_etl_pmda.utils_scraping.requests.Session.request") as mock_request:
        mock_resp = MagicMock()
        mock_resp.encoding = "ISO-8859-1"
        mock_resp.apparent_encoding = "Shift_JIS"
        mock_request.return_value = mock_resp

        resp = fetch_url("http://example.com")

        assert resp.encoding == "Shift_JIS"


def test_fetch_url_error() -> None:
    with patch("coreason_etl_pmda.utils_scraping.requests.Session.request") as mock_request:
        mock_request.side_effect = requests.RequestException("Boom")

        with pytest.raises(requests.RequestException):
            fetch_url("http://example.com")


def test_get_soup() -> None:
    mock_resp = MagicMock()
    mock_resp.content = b"<html><body>Hello</body></html>"

    soup = get_soup(mock_resp)
    assert isinstance(soup, BeautifulSoup)
    assert soup.find("body").text == "Hello"
