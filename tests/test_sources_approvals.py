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

from coreason_etl_pmda.sources_approvals import approvals_source


def test_approvals_source_scraping_japanese() -> None:
    # HTML with Japanese headers
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th>承認年月日</th>
                    <th>販売名</th>
                    <th>一般的名称</th>
                    <th>申請者氏名</th>
                    <th>審査報告書</th>
                    <th>薬効分類名</th>
                </tr>
                <tr>
                    <td>令和2年1月1日</td>
                    <td>ブランドA</td>
                    <td>ジェネリックA</td>
                    <td>会社A</td>
                    <td><a href="report_a.pdf">PDF</a></td>
                    <td>効能A</td>
                </tr>
                <tr>
                    <td>令和2年2月1日</td>
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
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        resource = approvals_source(url="http://example.com/jp")
        data = list(resource)

        assert len(data) == 2

        # Row 1
        assert data[0]["brand_name_jp"] == "ブランドA"
        assert data[0]["generic_name_jp"] == "ジェネリックA"
        assert data[0]["approval_date"] == "令和2年1月1日"
        assert data[0]["review_report_url"] == "http://example.com/report_a.pdf"
        assert data[0]["indication"] == "効能A"

        # Row 2
        assert data[1]["brand_name_jp"] == "ブランドB"
        assert data[1]["review_report_url"] is None


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
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        assert data[0]["brand_name_jp"] == "Name"


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
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 2
        assert data[0]["brand_name_jp"] == "A"
        assert data[1]["brand_name_jp"] == "B"


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
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        assert data[0]["brand_name_jp"] == "A"
