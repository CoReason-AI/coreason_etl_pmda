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
from coreason_etl_pmda.sources.review_reports import review_reports_source


@pytest.fixture  # type: ignore[misc]
def mock_fetch_url() -> Iterator[MagicMock]:
    # We must patch fetch_url in both modules where it is imported.
    # review_reports.py imports it, and common.py imports it.
    with patch("coreason_etl_pmda.sources.common.fetch_url") as mock_common:
        with patch("coreason_etl_pmda.sources.review_reports.fetch_url", new=mock_common):
            yield mock_common


@pytest.fixture  # type: ignore[misc]
def mock_logger() -> Iterator[MagicMock]:
    with patch("coreason_etl_pmda.utils_logger.logger") as mock:
        yield mock


def test_review_reports_source_extraction(mock_fetch_url: MagicMock) -> None:
    """Test extracting PDF links and downloading them."""

    # Mock Main Page
    html = """
    <html>
        <body>
            <table>
                <tr>
                    <th>販売名</th>
                    <th>審査報告書</th>
                </tr>
                <tr>
                    <td>Drug A</td>
                    <td>
                        <a href="report1.pdf">Part 1</a>
                        <a href="report2.pdf">Part 2</a>
                    </td>
                </tr>
                <tr>
                    <td>Drug B</td>
                    <td>-</td>
                </tr>
            </table>
        </body>
    </html>
    """

    mock_main_resp = MagicMock()
    mock_main_resp.content = html.encode("utf-8")
    mock_main_resp.encoding = "utf-8"
    mock_main_resp.original_encoding = "utf-8"  # Helper for get_soup

    # Mock PDF Responses
    mock_pdf1 = MagicMock()
    mock_pdf1.content = b"%PDF-1.4 ... Part 1"

    mock_pdf2 = MagicMock()
    mock_pdf2.content = b"%PDF-1.4 ... Part 2"

    def get_side_effect(url: str, *args: list[object], **kwargs: dict[str, object]) -> MagicMock:
        if "0001.html" in url:
            return mock_main_resp
        if "report1.pdf" in url:
            return mock_pdf1
        if "report2.pdf" in url:
            return mock_pdf2
        return MagicMock()

    mock_fetch_url.side_effect = get_side_effect

    source = review_reports_source()
    data = list(source)

    assert len(data) == 2

    item1 = data[0]
    assert item1["source_id"].endswith("report1.pdf")
    assert item1["raw_payload"]["brand_name_jp"] == "Drug A"
    assert item1["raw_payload"]["part_index"] == 1
    assert item1["raw_payload"]["content"] == b"%PDF-1.4 ... Part 1"

    item2 = data[1]
    assert item2["source_id"].endswith("report2.pdf")
    assert item2["raw_payload"]["part_index"] == 2


def test_review_reports_source_deduplication(mock_fetch_url: MagicMock) -> None:
    """Test deduplication logic using dlt state."""

    # Mock Main Page with one PDF
    html = """
    <html>
        <body>
            <table>
                <tr>
                    <th>販売名</th>
                    <th>審査報告書</th>
                </tr>
                <tr>
                    <td>Drug A</td>
                    <td><a href="http://example.com/report1.pdf">Part 1</a></td>
                </tr>
            </table>
        </body>
    </html>
    """

    mock_main_resp = MagicMock()
    mock_main_resp.content = html.encode("utf-8")
    mock_main_resp.encoding = "utf-8"
    mock_main_resp.original_encoding = "utf-8"

    mock_pdf = MagicMock()
    mock_pdf.content = b"PDF"

    def get_side_effect(url: str, *args: list[object], **kwargs: dict[str, object]) -> MagicMock:
        if "0001.html" in url:
            return mock_main_resp
        if "report1.pdf" in url:
            return mock_pdf
        return MagicMock()

    mock_fetch_url.side_effect = get_side_effect

    # Mock State
    with patch("dlt.current.source_state") as mock_state:
        # Pre-populate state with report1.pdf
        state_dict = {"downloaded_ids": {"http://example.com/report1.pdf": 123456}}
        mock_state.return_value = state_dict

        source = review_reports_source()
        data = list(source)

        # Should be skipped
        assert len(data) == 0

        # Verify fetch_url was NOT called for PDF (only main page)
        calls = mock_fetch_url.call_args_list
        # Should contain main url, but NOT report1.pdf
        assert any("0001.html" in str(c) for c in calls)
        assert not any("report1.pdf" in str(c) for c in calls)


def test_review_reports_source_no_table(mock_fetch_url: MagicMock) -> None:
    """Test when no table matches headers."""
    html = """
    <html>
        <body>
            <table>
                <tr><th>Other</th></tr>
            </table>
        </body>
    </html>
    """
    mock_resp = MagicMock()
    mock_resp.content = html.encode("utf-8")
    mock_resp.encoding = "utf-8"
    mock_resp.original_encoding = "utf-8"
    mock_fetch_url.return_value = mock_resp

    source = review_reports_source()
    data = list(source)

    assert len(data) == 0


def test_review_reports_source_download_error(mock_fetch_url: MagicMock) -> None:
    """Test error handling during PDF download."""
    html = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>審査報告書</th></tr>
                <tr><td>Drug A</td><td><a href="error.pdf">Link</a></td></tr>
            </table>
        </body>
    </html>
    """
    mock_main = MagicMock()
    mock_main.content = html.encode("utf-8")
    mock_main.encoding = "utf-8"
    mock_main.original_encoding = "utf-8"

    def get_side_effect(url: str, *args: list[object], **kwargs: dict[str, object]) -> MagicMock:
        if "0001.html" in url:
            return mock_main
        if "error.pdf" in url:
            raise Exception("Download Failed")
        return MagicMock()

    mock_fetch_url.side_effect = get_side_effect

    source = review_reports_source()
    data = list(source)

    assert len(data) == 0


def test_review_reports_source_missing_report_column(mock_fetch_url: MagicMock, mock_logger: MagicMock) -> None:
    """Test when table matches keywords but missing report column."""
    # Matches: 販売名, 一般的名称. But report column missing.
    html = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>その他</th></tr>
            </table>
        </body>
    </html>
    """
    mock_resp = MagicMock()
    mock_resp.content = html.encode("utf-8")
    mock_resp.encoding = "utf-8"
    mock_resp.original_encoding = "utf-8"
    mock_fetch_url.return_value = mock_resp

    source = review_reports_source()
    data = list(source)
    assert len(data) == 0


def test_review_reports_source_missing_brand_column(mock_fetch_url: MagicMock, mock_logger: MagicMock) -> None:
    """Test when table matches keywords but missing brand column."""
    # Matches: 審査報告書, 一般的名称. But brand column missing.
    html = """
    <html>
        <body>
            <table>
                <tr><th>審査報告書</th><th>一般的名称</th><th>その他</th></tr>
            </table>
        </body>
    </html>
    """
    mock_resp = MagicMock()
    mock_resp.content = html.encode("utf-8")
    mock_resp.encoding = "utf-8"
    mock_resp.original_encoding = "utf-8"
    mock_fetch_url.return_value = mock_resp

    source = review_reports_source()
    data = list(source)
    assert len(data) == 0


def test_review_reports_source_row_missing_cells(mock_fetch_url: MagicMock) -> None:
    """Test row with insufficient cells."""
    html = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>審査報告書</th></tr>
                <tr><td>Drug A</td></tr>
            </table>
        </body>
    </html>
    """
    # 2nd row has only 1 cell.
    mock_resp = MagicMock()
    mock_resp.content = html.encode("utf-8")
    mock_resp.encoding = "utf-8"
    mock_resp.original_encoding = "utf-8"
    mock_fetch_url.return_value = mock_resp

    source = review_reports_source()
    data = list(source)
    assert len(data) == 0


def test_review_reports_source_row_no_links(mock_fetch_url: MagicMock) -> None:
    """Test row with report cell but no links."""
    html = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>審査報告書</th></tr>
                <tr><td>Drug A</td><td>No Link</td></tr>
            </table>
        </body>
    </html>
    """
    mock_resp = MagicMock()
    mock_resp.content = html.encode("utf-8")
    mock_resp.encoding = "utf-8"
    mock_resp.original_encoding = "utf-8"
    mock_fetch_url.return_value = mock_resp

    source = review_reports_source()
    data = list(source)
    assert len(data) == 0


def test_review_reports_source_ignore_non_pdf(mock_fetch_url: MagicMock) -> None:
    """Test ignoring non-pdf links."""
    html = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>審査報告書</th></tr>
                <tr><td>Drug A</td><td><a href="page.html">HTML</a></td></tr>
            </table>
        </body>
    </html>
    """
    mock_resp = MagicMock()
    mock_resp.content = html.encode("utf-8")
    mock_resp.encoding = "utf-8"
    mock_resp.original_encoding = "utf-8"
    mock_fetch_url.return_value = mock_resp

    source = review_reports_source()
    data = list(source)
    assert len(data) == 0


def test_review_reports_source_empty_table(mock_fetch_url: MagicMock) -> None:
    """Test table with no rows."""
    html = """
    <html>
        <body>
            <table>
            </table>
        </body>
    </html>
    """
    mock_resp = MagicMock()
    mock_resp.content = html.encode("utf-8")
    mock_resp.encoding = "utf-8"
    mock_resp.original_encoding = "utf-8"
    mock_fetch_url.return_value = mock_resp

    source = review_reports_source()
    data = list(source)
    assert len(data) == 0
