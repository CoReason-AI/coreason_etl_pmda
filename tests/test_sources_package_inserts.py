# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from coreason_etl_pmda.sources_package_inserts import package_inserts_source


@pytest.fixture  # type: ignore[misc]
def mock_requests_session() -> Iterator[MagicMock]:
    with patch("coreason_etl_pmda.sources_package_inserts.requests.Session") as mock:
        yield mock


def test_package_inserts_source_no_results(mock_requests_session: MagicMock) -> None:
    """Test that source handles no results gracefully."""
    session_instance = mock_requests_session.return_value

    # Mock Search Response (No results)
    mock_response = MagicMock()
    mock_response.text = "該当するデータはありません"  # "No matching data"
    mock_response.content = b"<html><body>No results</body></html>"
    session_instance.post.return_value = mock_response

    source = package_inserts_source(start_date="20230101", end_date="20230102")
    data = list(source)

    assert len(data) == 0
    session_instance.post.assert_called_once()


def test_package_inserts_source_with_results(mock_requests_session: MagicMock) -> None:
    """Test that source parses results and follows links."""
    session_instance = mock_requests_session.return_value

    # Mock Search Response (1 result row)
    search_html = """
    <html>
        <body>
            <table>
                <tr class="row1">
                    <td>
                        <a href="/PmdaSearch/iyakuDetail/GeneralList/123456">Detail</a>
                    </td>
                    <td>Drug A</td>
                </tr>
            </table>
        </body>
    </html>
    """
    mock_search_resp = MagicMock()
    mock_search_resp.text = "Results found"
    mock_search_resp.content = search_html.encode("utf-8")
    session_instance.post.return_value = mock_search_resp

    # Mock Detail Page Response (Contains XML link)
    detail_html = """
    <html>
        <body>
            <h1>Drug A</h1>
            <a href="http://example.com/doc.xml">XML</a>
        </body>
    </html>
    """
    mock_detail_resp = MagicMock()
    mock_detail_resp.content = detail_html.encode("utf-8")

    # Mock Content Response
    mock_content_resp = MagicMock()
    mock_content_resp.text = "<xml>Content</xml>"
    mock_content_resp.content = b"<xml>Content</xml>"
    mock_content_resp.encoding = "utf-8"

    # Configure side_effect for session.get
    # 1. Initial GET (base_url) - ignore
    # 2. Detail Page
    # 3. Content

    def get_side_effect(url: str, *args: list[object], **kwargs: dict[str, object]) -> MagicMock:
        if "iyakuDetail" in url:
            return mock_detail_resp
        if url.endswith(".xml"):
            return mock_content_resp
        # Default for base url
        return MagicMock()

    session_instance.get.side_effect = get_side_effect

    source = package_inserts_source(start_date="20230101", end_date="20230102")
    data = list(source)

    assert len(data) == 1
    item = data[0]
    assert item["source_id"] == "http://example.com/doc.xml"
    assert item["raw_payload"]["content"] == b"<xml>Content</xml>"
    assert item["raw_payload"]["source_url"].endswith("123456")


def test_package_inserts_pagination(mock_requests_session: MagicMock) -> None:
    """Test pagination logic."""
    session_instance = mock_requests_session.return_value

    # Page 1 HTML
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

    # Page 2 HTML
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

    # Mock Detail/Content (Generic)
    mock_detail = MagicMock()
    mock_detail.content = b'<html><a href="doc.xml">XML</a></html>'

    mock_content = MagicMock()
    mock_content.content = b"XML Content"

    session_instance.post.return_value = mock_page1

    def get_side_effect(url: str, *args: list[object], **kwargs: dict[str, object]) -> MagicMock:
        if "page2" in url:
            return mock_page2
        if "iyakuDetail" in url:
            return mock_detail
        if "doc.xml" in url:
            return mock_content
        return MagicMock()

    session_instance.get.side_effect = get_side_effect

    source = package_inserts_source()
    data = list(source)

    assert len(data) == 2
    ids = [d["raw_payload"]["source_url"].split("/")[-1] for d in data]
    assert "1" in ids
    assert "2" in ids


def test_package_inserts_fallback_link(mock_requests_session: MagicMock) -> None:
    """Test finding link by text when extension check fails."""
    session_instance = mock_requests_session.return_value

    mock_search_resp = MagicMock()
    mock_search_resp.text = "Results"
    mock_search_resp.content = (
        b'<html><table><tr><td><a href="/PmdaSearch/iyakuDetail/GeneralList/3">Detail</a></td></tr></table></html>'
    )
    session_instance.post.return_value = mock_search_resp

    # Detail page with no .xml link but a link with text "添付文書" (assuming it points to HTML/SGML)
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

    def get_side_effect(url: str, *args: list[object], **kwargs: dict[str, object]) -> MagicMock:
        if "iyakuDetail" in url:
            return mock_detail_resp
        if "/view/doc" in url:
            return mock_content_resp
        return MagicMock()

    session_instance.get.side_effect = get_side_effect

    source = package_inserts_source()
    data = list(source)

    assert len(data) == 1
    assert data[0]["source_id"].endswith("/view/doc")


def test_package_inserts_error_handling(mock_requests_session: MagicMock) -> None:
    """Test error handling when detail page fetch fails."""
    session_instance = mock_requests_session.return_value

    mock_search_resp = MagicMock()
    mock_search_resp.text = "Results"
    mock_search_resp.content = (
        b'<html><table><tr><td><a href="/PmdaSearch/iyakuDetail/GeneralList/4">Detail</a></td></tr></table></html>'
    )
    session_instance.post.return_value = mock_search_resp

    # Detail page fetch raises exception
    def get_side_effect(url: str, *args: list[object], **kwargs: dict[str, object]) -> MagicMock:
        if "iyakuDetail" in url:
            raise Exception("Network Error")
        return MagicMock()

    session_instance.get.side_effect = get_side_effect

    source = package_inserts_source()
    data = list(source)

    # Should yield 0 items but not crash
    assert len(data) == 0
