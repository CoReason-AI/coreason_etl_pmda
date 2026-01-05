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
from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources.helpers import requests

from coreason_etl_pmda.sources.approvals import approvals_source


@pytest.fixture  # type: ignore[misc]
def mock_state() -> Generator[MagicMock, None, None]:
    with patch("dlt.current.source_state", return_value={}) as m:
        yield m


def test_approvals_source_scraping_japanese(mock_state: MagicMock) -> None:
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

    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        resource = approvals_source(url="http://example.com/jp")
        data = list(resource)

        assert len(data) == 2

        # Row 1
        payload1 = data[0]["raw_payload"]
        assert payload1["販売名"] == "ブランドA"
        assert payload1["承認番号"] == "123456"
        assert data[0]["source_id"] == "123456"
        assert payload1["review_report_url"] == "http://example.com/report_a.pdf"

        # Row 2
        payload2 = data[1]["raw_payload"]
        assert payload2["販売名"] == "ブランドB"
        assert payload2["承認番号"] == "789012"
        assert data[1]["source_id"] == "789012"
        assert "review_report_url" not in payload2


def test_approvals_source_incremental() -> None:
    """Test High-Water Mark (incremental loading) logic."""
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>承認番号</th><th>承認年月日</th><th>一般的名称</th></tr>
                <tr><td>A</td><td>1001</td><td>D1</td><td>G1</td></tr>
                <tr><td>B</td><td>1002</td><td>D2</td><td>G2</td></tr>
            </table>
        </body>
    </html>
    """

    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        # 1. First Run
        state: dict[str, Any] = {}
        with patch("dlt.current.source_state", return_value=state):
            data1 = list(approvals_source())
            assert len(data1) == 2
            assert data1[0]["source_id"] == "1001"
            assert data1[1]["source_id"] == "1002"
            assert "seen_ids" in state
            assert "1001" in state["seen_ids"]
            assert "1002" in state["seen_ids"]

        # 2. Second Run (Same data)
        with patch("dlt.current.source_state", return_value=state):
            data2 = list(approvals_source())
            assert len(data2) == 0

        # 3. Third Run (New data)
        html_content_new = """
        <html>
            <body>
                <table>
                    <tr><th>販売名</th><th>承認番号</th><th>承認年月日</th><th>一般的名称</th></tr>
                    <tr><td>A</td><td>1001</td><td>D1</td><td>G1</td></tr>
                    <tr><td>C</td><td>1003</td><td>D3</td><td>G3</td></tr>
                </table>
            </body>
        </html>
        """
        mock_resp.content = html_content_new.encode("utf-8")

        with patch("dlt.current.source_state", return_value=state):
            data3 = list(approvals_source())
            assert len(data3) == 1
            assert data3[0]["source_id"] == "1003"
            assert "1003" in state["seen_ids"]
            assert "1001" in state["seen_ids"]


def test_approvals_source_fallback_id(mock_state: MagicMock) -> None:
    """Test fallback ID generation when Approval Number is missing."""
    # Ensure enough headers to match heuristic (need >= 2 matches from keywords)
    # Keywords: 販売名, 一般的名称, 承認年月日, 承認番号
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>承認年月日</th><th>一般的名称</th></tr>
                <tr><td>BrandX</td><td>2020-01-01</td><td>GenX</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        # ID should be hash
        import hashlib

        raw = "BrandX|2020-01-01"
        expected_id = hashlib.md5(raw.encode("utf-8")).hexdigest()
        assert data[0]["source_id"] == expected_id


def test_approvals_source_application_type_override(mock_state: MagicMock) -> None:
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th><th>承認番号</th></tr>
                <tr><td>BrandG</td><td>GenG</td><td>R2.1.1</td><td>999</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        # Override application_type
        resource = approvals_source(url="http://example.com/generic", application_type="Generic")
        data = list(resource)

        assert len(data) == 1
        assert data[0]["raw_payload"]["application_type"] == "Generic"
        assert data[0]["source_id"] == "999"


def test_approvals_source_whitespace_japanese(mock_state: MagicMock) -> None:
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
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        payload = data[0]["raw_payload"]
        assert payload["販売名"] == "Name"


def test_approvals_source_multiple_tables_japanese(mock_state: MagicMock) -> None:
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
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
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


def test_approvals_source_ignore_irrelevant_tables(mock_state: MagicMock) -> None:
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
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        payload = data[0]["raw_payload"]
        assert payload["販売名"] == "A"


def test_approvals_source_network_error(mock_state: MagicMock) -> None:
    """Test that HTTP errors are raised."""
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_resp

        with pytest.raises((ResourceExtractionError, requests.HTTPError)):
            list(approvals_source())


def test_approvals_source_shift_jis_encoding(mock_state: MagicMock) -> None:
    """Test handling of Shift_JIS encoded content."""
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
    # Create bytes using shift_jis
    html_bytes = html_str.encode("shift_jis")

    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_bytes
        mock_resp.encoding = "shift_jis"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        payload = data[0]["raw_payload"]
        assert payload["販売名"] == "アスピリン"
        assert data[0]["original_encoding"] == "shift_jis"


def test_approvals_source_colspan_skip(mock_state: MagicMock) -> None:
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
                <tr><td>B</td><td>G2</td><td>D2</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        # Should match A and B, skip Summary info
        assert len(data) == 2
        assert data[0]["raw_payload"]["販売名"] == "A"
        assert data[1]["raw_payload"]["販売名"] == "B"


def test_approvals_source_relative_links(mock_state: MagicMock) -> None:
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
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source(url=base_url))
        assert len(data) == 1
        expected = "https://www.pmda.go.jp/reports/file.pdf"
        assert data[0]["raw_payload"]["review_report_url"] == expected


def test_approvals_source_unknown_encoding_fallback(mock_state: MagicMock) -> None:
    """Test fallback when response.encoding is None but BS4 detects it."""
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
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = None  # None case
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        # BS4 detects utf-8 from the content
        assert data[0]["original_encoding"] == "utf-8"


def test_approvals_source_multiple_review_links(mock_state: MagicMock) -> None:
    """Test behavior when multiple links exist in Review Report column (e.g. Part 1, Part 2)."""
    # Should pick the first one
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>販売名</th><th>一般的名称</th><th>承認年月日</th><th>審査報告書</th></tr>
                <tr>
                    <td>A</td><td>G</td><td>D</td>
                    <td>
                        <a href="part1.pdf">Part 1</a>
                        <br>
                        <a href="part2.pdf">Part 2</a>
                    </td>
                </tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources.approvals.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source(url="http://example.com/"))
        assert len(data) == 1
        assert data[0]["raw_payload"]["review_report_url"] == "http://example.com/part1.pdf"
