# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests
from coreason_etl_pmda.sources.package_inserts import package_inserts_source


@pytest.fixture  # type: ignore[misc]
def mock_fetch_url() -> Generator[MagicMock, None, None]:
    with patch("coreason_etl_pmda.sources.package_inserts.fetch_url") as m:
        yield m


@pytest.fixture  # type: ignore[misc]
def mock_get_session() -> Generator[MagicMock, None, None]:
    with patch("coreason_etl_pmda.sources.package_inserts.get_session") as m:
        yield m


def test_package_inserts_pagination(mock_fetch_url: MagicMock, mock_get_session: MagicMock) -> None:
    """Test pagination logic (moving to 'Next' page)."""

    # Page 1 HTML (with Next link)
    page1_html = """
    <html>
        <body>
            <table>
                <tr><td><a href="/PmdaSearch/iyakuDetail/GeneralList/1">Item 1</a></td></tr>
            </table>
            <a href="/PmdaSearch/iyakuSearch/page2">次へ</a>
        </body>
    </html>
    """

    # Page 2 HTML (No Next link)
    page2_html = """
    <html>
        <body>
            <table>
                <tr><td><a href="/PmdaSearch/iyakuDetail/GeneralList/2">Item 2</a></td></tr>
            </table>
        </body>
    </html>
    """

    mock_page1 = MagicMock()
    mock_page1.content = page1_html.encode("utf-8")
    mock_page1.text = "Results"

    mock_page2 = MagicMock()
    mock_page2.content = page2_html.encode("utf-8")
    mock_page2.text = "Results"

    # Mock Detail/Content (Generic)
    mock_detail = MagicMock()
    mock_detail.content = b'<html><a href="doc.xml">XML</a></html>'

    mock_content = MagicMock()
    mock_content.content = b"XML Content"

    def fetch_side_effect(
        url: str, session: requests.Session | None = None, method: str = "GET", **kwargs: Any
    ) -> MagicMock:
        if "page2" in url:
            return mock_page2
        if "iyakuDetail" in url:
            return mock_detail
        if "doc.xml" in url:
            return mock_content
        # Initial search (POST) or first GET
        if method == "POST":
            return mock_page1
        return mock_page1  # default fallback for initial GET

    mock_fetch_url.side_effect = fetch_side_effect

    source = package_inserts_source()
    data = list(source)

    assert len(data) == 2
    ids = [d["raw_payload"]["source_url"].split("/")[-1] for d in data]
    assert "1" in ids
    assert "2" in ids


def test_package_inserts_fallback_link(mock_fetch_url: MagicMock, mock_get_session: MagicMock) -> None:
    """Test finding link by text when extension check fails."""

    mock_search_resp = MagicMock()
    mock_search_resp.text = "Results"
    mock_search_resp.content = (
        b'<html><table><tr><td><a href="/PmdaSearch/iyakuDetail/GeneralList/3">Detail</a></td></tr></table></html>'
    )

    # Detail page with no .xml link but a link with text "添付文書"
    detail_html = """
    <html>
        <body>
            <a href="/view/doc">添付文書</a>
        </body>
    </html>
    """
    mock_detail_resp = MagicMock()
    mock_detail_resp.content = detail_html.encode("utf-8")

    mock_content_resp = MagicMock()
    mock_content_resp.content = b"Content"
    mock_content_resp.encoding = "utf-8"

    def fetch_side_effect(url: str, **kwargs: Any) -> MagicMock:
        if "iyakuDetail" in url:
            return mock_detail_resp
        if "/view/doc" in url:
            return mock_content_resp
        return mock_search_resp

    mock_fetch_url.side_effect = fetch_side_effect

    source = package_inserts_source()
    data = list(source)

    assert len(data) == 1
    assert data[0]["source_id"].endswith("/view/doc")


def test_package_inserts_priority_logic(mock_fetch_url: MagicMock, mock_get_session: MagicMock) -> None:
    """Test link selection priority (XML > SGML > HTML)."""

    # Search result
    mock_search_resp = MagicMock()
    mock_search_resp.text = "Results found"
    mock_search_resp.content = (
        b'<html><table><tr><td><a href="/PmdaSearch/iyakuDetail/GeneralList/1">Detail</a></td></tr></table></html>'
    )

    # Detail page with all types
    # Case 1: XML exists
    detail_html_all = """
    <html>
        <body>
            <a href="doc.html">HTML</a>
            <a href="doc.xml">XML</a>
            <a href="doc.sgml">SGML</a>
        </body>
    </html>
    """
    mock_detail_all = MagicMock()
    mock_detail_all.content = detail_html_all.encode("utf-8")

    mock_content = MagicMock()
    mock_content.content = b"Content"

    def fetch_side_effect_all(url: str, **kwargs: Any) -> MagicMock:
        if "iyakuDetail" in url:
            return mock_detail_all
        return mock_search_resp if "iyakuSearch" in url else mock_content

    mock_fetch_url.side_effect = fetch_side_effect_all

    source = package_inserts_source()
    data = list(source)
    assert len(data) == 1
    assert data[0]["source_id"].endswith(".xml")

    # Case 2: No XML, only SGML and HTML
    detail_html_sgml = """
    <html>
        <body>
            <a href="doc.html">HTML</a>
            <a href="doc.sgml">SGML</a>
        </body>
    </html>
    """
    mock_detail_sgml = MagicMock()
    mock_detail_sgml.content = detail_html_sgml.encode("utf-8")

    def fetch_side_effect_sgml(url: str, **kwargs: Any) -> MagicMock:
        if "iyakuDetail" in url:
            return mock_detail_sgml
        return mock_search_resp if "iyakuSearch" in url else mock_content

    mock_fetch_url.side_effect = fetch_side_effect_sgml

    source = package_inserts_source()
    data = list(source)
    assert len(data) == 1
    assert data[0]["source_id"].endswith(".sgml")


def test_package_inserts_search_error(mock_fetch_url: MagicMock, mock_get_session: MagicMock) -> None:
    """Test handling when the initial search POST request fails."""

    def fetch_side_effect(url: str, method: str = "GET", **kwargs: Any) -> MagicMock:
        if method == "POST":
            raise Exception("Search Connection Error")
        return MagicMock()  # Initial GET

    mock_fetch_url.side_effect = fetch_side_effect

    source = package_inserts_source()
    with pytest.raises(Exception, match="Search Connection Error"):
        list(source)


def test_package_inserts_detail_malformed(mock_fetch_url: MagicMock, mock_get_session: MagicMock) -> None:
    """Test handling of detail page with no valid links."""

    mock_search_resp = MagicMock()
    mock_search_resp.text = "Results"
    mock_search_resp.content = (
        b'<html><table><tr><td><a href="/PmdaSearch/iyakuDetail/GeneralList/1">Detail</a></td></tr></table></html>'
    )

    # Detail page with NO valid links
    mock_detail = MagicMock()
    mock_detail.content = b"<html><body>No links here</body></html>"

    def fetch_side_effect(url: str, **kwargs: Any) -> MagicMock:
        if "iyakuDetail" in url:
            return mock_detail
        return mock_search_resp

    mock_fetch_url.side_effect = fetch_side_effect

    source = package_inserts_source()
    data = list(source)

    # Should gracefully skip this item and yield nothing
    assert len(data) == 0


def test_package_inserts_process_detail_exception(mock_fetch_url: MagicMock, mock_get_session: MagicMock) -> None:
    """Test that if _process_detail_page fails, we catch the exception and continue."""

    mock_search_resp = MagicMock()
    mock_search_resp.text = "Results"
    mock_search_resp.content = (
        b'<html><table><tr><td><a href="/PmdaSearch/iyakuDetail/GeneralList/1">Detail</a></td></tr></table></html>'
    )

    def fetch_side_effect(url: str, **kwargs: Any) -> MagicMock:
        if "iyakuDetail" in url:
            raise Exception("Detail Fetch Failed")
        return mock_search_resp

    mock_fetch_url.side_effect = fetch_side_effect

    source = package_inserts_source()

    with patch("coreason_etl_pmda.sources.package_inserts.logger.exception") as mock_log:
        data = list(source)
        assert len(data) == 0
        mock_log.assert_called()


def test_package_inserts_no_data(mock_fetch_url: MagicMock, mock_get_session: MagicMock) -> None:
    """Test handling when no data is found."""

    mock_search_resp = MagicMock()
    mock_search_resp.text = "該当するデータはありません"
    mock_search_resp.content = b"No data"

    mock_fetch_url.return_value = mock_search_resp

    source = package_inserts_source()
    data = list(source)
    assert len(data) == 0
