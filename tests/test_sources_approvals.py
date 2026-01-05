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
from coreason_etl_pmda.sources_approvals import approvals_source
from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources.helpers import requests


def test_approvals_source_scraping_japanese() -> None:
    # HTML with Japanese headers
    # Include 承認番号 (Approval Number)
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th>承認年月日</th>
                    <th>承認番号</th>
                    <th>販売名</th>
                    <th>一般的名称</th>
                    <th>申請者氏名</th>
                    <th>審査報告書</th>
                    <th>薬効分類名</th>
                </tr>
                <tr>
                    <td>令和2年1月1日</td>
                    <td>123456</td>
                    <td>ブランドA</td>
                    <td>ジェネリックA</td>
                    <td>会社A</td>
                    <td><a href="report_a.pdf">PDF</a></td>
                    <td>効能A</td>
                </tr>
                <tr>
                    <td>令和2年2月1日</td>
                    <td>789012</td>
                    <td>ブランドB</td>
                    <td>ジェネリックB</td>
                    <td>会社B</td>
                    <td>-</td>
                    <td>効能B</td>
                </tr>
            </table>
        </body>
    </html>
    """

    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        resource = approvals_source(url="http://example.com/jp")
        data = list(resource)

        assert len(data) == 2

        # Verify Envelope
        for row in data:
            assert "source_id" in row
            assert row["source_id"] == "http://example.com/jp"
            assert "ingestion_ts" in row
            assert "raw_payload" in row
            assert row["original_encoding"] == "utf-8"

        # Row 1
        payload1 = data[0]["raw_payload"]
        # Expect Japanese keys
        assert payload1["販売名"] == "ブランドA"
        assert payload1["一般的名称"] == "ジェネリックA"
        assert payload1["承認年月日"] == "令和2年1月1日"
        assert payload1["承認番号"] == "123456"
        assert payload1["review_report_url"] == "http://example.com/report_a.pdf"
        assert payload1["薬効分類名"] == "効能A"
        assert payload1["application_type"] == "New Drug"  # Default

        # Row 2
        payload2 = data[1]["raw_payload"]
        assert payload2["販売名"] == "ブランドB"
        assert payload2["承認番号"] == "789012"
        # review_report_url might not be present if no link found?
        # Code logic: `if review_url: record["review_report_url"] = review_url`
        assert "review_report_url" not in payload2
        assert payload2["application_type"] == "New Drug"


def test_approvals_source_application_type_override() -> None:
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th></tr>
                <tr><td>BrandG</td><td>GenG</td><td>R2.1.1</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        # Override application_type
        resource = approvals_source(url="http://example.com/generic", application_type="Generic")
        data = list(resource)

        assert len(data) == 1
        assert data[0]["raw_payload"]["application_type"] == "Generic"
        assert data[0]["raw_payload"]["販売名"] == "BrandG"


def test_approvals_source_whitespace_japanese() -> None:
    """Test robustness against whitespace in Japanese headers."""
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th> 承認  年月日 </th>
                    <th>販\n売\n名</th>
                    <th>一般的名称</th>
                </tr>
                <tr>
                    <td>R2.1.1</td>
                    <td>Name</td>
                    <td>Gen</td>
                </tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        payload = data[0]["raw_payload"]
        # Code logic strips whitespace from header
        assert payload["販売名"] == "Name"


def test_approvals_source_multiple_tables_japanese() -> None:
    """Test multiple tables on Japanese page."""
    html_content = """
    <html>
        <body>
            <h1>新薬</h1>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th></tr>
                <tr><td>A</td><td>G</td><td>D</td></tr>
            </table>
            <h1>後発品</h1>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th></tr>
                <tr><td>B</td><td>G2</td><td>D2</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 2
        payload1 = data[0]["raw_payload"]
        payload2 = data[1]["raw_payload"]
        assert payload1["販売名"] == "A"
        assert payload2["販売名"] == "B"


def test_approvals_source_ignore_irrelevant_tables() -> None:
    html_content = """
    <html>
        <body>
            <!-- Truly empty table to hit 'if not header_row: continue' -->
            <table id="empty">
            </table>

            <table><tr><th>Other</th></tr><tr><td>1</td></tr></table>
            <table>
                <!-- Table with headers but mismatched cells -->
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th></tr>
                <tr><td>OnlyOneCell</td></tr>
            </table>
             <table>
                <!-- Empty header row -->
                <tr></tr>
             </table>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th></tr>
                <tr><td>A</td><td>G</td><td>D</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        payload = data[0]["raw_payload"]
        assert payload["販売名"] == "A"


def test_approvals_source_network_error() -> None:
    """Test that HTTP errors are raised."""
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_resp

        # dlt wraps exceptions in ResourceExtractionError
        with pytest.raises(ResourceExtractionError) as excinfo:
            list(approvals_source())
        assert "404 Not Found" in str(excinfo.value)


def test_approvals_source_shift_jis_encoding() -> None:
    """Test handling of Shift_JIS encoded content."""
    # Construct minimal HTML in bytes
    html_str = """
    <html>
        <head><meta charset="Shift_JIS"></head>
        <body>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th></tr>
                <tr><td>アスピリン</td><td>ジェネリック</td><td>2020</td></tr>
            </table>
        </body>
    </html>
    """
    html_bytes = html_str.encode("shift_jis")

    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_bytes
        mock_resp.encoding = "shift_jis"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        payload = data[0]["raw_payload"]
        # BeautifulSoup should have decoded it correctly
        assert payload["販売名"] == "アスピリン"
        assert data[0]["original_encoding"] == "shift_jis"


def test_approvals_source_colspan_skip() -> None:
    """Test that rows with colspan/rowspan (mismatched cell count) are skipped."""
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th></tr>
                <!-- Normal Row -->
                <tr><td>A</td><td>G</td><td>D</td></tr>
                <!-- Colspan Row (only 1 cell, headers expect 3) -->
                <tr><td colspan="3">Summary info</td></tr>
                <!-- Rowspan might cause issues if not handled by simple iteration,
                     but here checking strict length mismatch. -->
                <tr><td>B</td><td>G2</td><td>D2</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        # Should match A and B, skip Summary info
        assert len(data) == 2
        assert data[0]["raw_payload"]["販売名"] == "A"
        assert data[1]["raw_payload"]["販売名"] == "B"


def test_approvals_source_relative_links() -> None:
    """Test that relative URLs are joined correctly."""
    base_url = "https://www.pmda.go.jp/drugs/"
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th><th>審査報告書</th></tr>
                <tr>
                    <td>A</td><td>G</td><td>D</td>
                    <td><a href="../reports/file.pdf">PDF</a></td>
                </tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source(url=base_url))
        assert len(data) == 1
        # urljoin('https://www.pmda.go.jp/drugs/', '../reports/file.pdf')
        # -> 'https://www.pmda.go.jp/reports/file.pdf'
        expected = "https://www.pmda.go.jp/reports/file.pdf"
        assert data[0]["raw_payload"]["review_report_url"] == expected


def test_approvals_source_unknown_encoding_fallback() -> None:
    """Test fallback when response.encoding is None."""
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th></tr>
                <tr><td>A</td><td>G</td><td>D</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = None  # None case
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        assert data[0]["original_encoding"] == "unknown"
