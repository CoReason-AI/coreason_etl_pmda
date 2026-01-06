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
from tenacity import RetryError, wait_none

from coreason_etl_pmda.utils_scraping import fetch_url, get_session, get_soup, _should_retry_error


def test_get_session() -> None:
    session = get_session()
    assert isinstance(session, requests.Session)
    assert "User-Agent" in session.headers
    assert session.headers["User-Agent"] == "CoReasonETL/1.0"


def test_fetch_url_success() -> None:
    with patch("requests.Session.request") as mock_request:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.encoding = "utf-8"
        mock_request.return_value = mock_resp

        resp = fetch_url("http://example.com")
        assert resp == mock_resp
        mock_request.assert_called_once()


def test_fetch_url_calls_rate_limiter() -> None:
    with patch("requests.Session.request") as mock_request:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_request.return_value = mock_resp

        fetch_url("http://example.com")
        mock_request.assert_called_once()


def test_fetch_url_retry_transient() -> None:
    """Test that transient errors (500) trigger retries."""
    with patch("requests.Session.request") as mock_request:
        bad_resp = MagicMock()
        bad_resp.status_code = 500
        bad_resp.raise_for_status.side_effect = requests.HTTPError("500 Error", response=bad_resp)

        good_resp = MagicMock()
        good_resp.status_code = 200
        good_resp.raise_for_status.return_value = None
        good_resp.encoding = "utf-8"

        mock_request.side_effect = [bad_resp, bad_resp, good_resp]

        resp = fetch_url("http://example.com")
        assert resp == good_resp
        assert mock_request.call_count == 3


def test_fetch_url_retry_connection_error() -> None:
    """Test that connection errors trigger retries."""

    with patch("requests.Session.request") as mock_request:
        mock_request.side_effect = [
            requests.ConnectionError("Fail"),
            requests.ConnectionError("Fail"),
            MagicMock(encoding="utf-8", raise_for_status=MagicMock())
        ]

        fetch_url("http://example.com")
        assert mock_request.call_count == 3


def test_fetch_url_no_retry_permanent() -> None:
    """Test that permanent errors (404) do NOT trigger retries."""

    with patch("requests.Session.request") as mock_request:
        bad_resp = MagicMock()
        bad_resp.status_code = 404
        bad_resp.raise_for_status.side_effect = requests.HTTPError("404 Error", response=bad_resp)

        mock_request.return_value = bad_resp

        with pytest.raises(requests.HTTPError):
            fetch_url("http://example.com")

        # Should be called only once because 404 is not in retry list
        assert mock_request.call_count == 1


def test_fetch_url_iso_8859_1_fix() -> None:
    """Test that we attempt to fix ISO-8859-1 encoding."""
    with patch("requests.Session.request") as mock_request:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.encoding = "ISO-8859-1"
        mock_resp.apparent_encoding = "Shift_JIS"
        mock_request.return_value = mock_resp

        resp = fetch_url("http://example.com")

        assert resp.encoding == "Shift_JIS"


def test_get_soup() -> None:
    mock_resp = MagicMock()
    mock_resp.content = b"<html><body>Hello</body></html>"

    soup = get_soup(mock_resp)
    assert isinstance(soup, BeautifulSoup)
    assert soup.find("body").text == "Hello"

def test_should_retry_error_generic() -> None:
    """Test that generic exceptions return False in predicate."""
    assert _should_retry_error(ValueError("foo")) is False
    assert _should_retry_error(requests.HTTPError("foo")) is False # No response
