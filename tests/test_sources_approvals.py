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

def test_approvals_source_scraping() -> None:
    # HTML with a table containing drug info and a PDF link
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th>Approval Date</th>
                    <th>Brand Name</th>
                    <th>Generic Name</th>
                    <th>Applicant</th>
                    <th>Review Report</th>
                    <th>Indication</th>
                </tr>
                <tr>
                    <td>2024-01-01</td>
                    <td>Brand A</td>
                    <td>Generic A</td>
                    <td>Pharma A</td>
                    <td><a href="report_a.pdf">PDF</a></td>
                    <td>Indication A</td>
                </tr>
                <tr>
                    <td>2024-02-01</td>
                    <td>Brand B</td>
                    <td>Generic B</td>
                    <td>Pharma B</td>
                    <td>-</td>
                    <td>Indication B</td>
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

        resource = approvals_source(url="http://example.com/approvals")
        data = list(resource)

        assert len(data) == 2

        # Row 1
        assert data[0]["brand_name_jp"] == "Brand A"
        assert data[0]["generic_name_jp"] == "Generic A"
        assert data[0]["approval_date"] == "2024-01-01"
        assert data[0]["review_report_url"] == "http://example.com/report_a.pdf"
        assert data[0]["indication"] == "Indication A"

        # Row 2 (No PDF)
        assert data[1]["brand_name_jp"] == "Brand B"
        assert data[1]["review_report_url"] is None
        assert data[1]["indication"] == "Indication B"

def test_approvals_source_ignore_irrelevant_tables() -> None:
    html_content = """
    <html>
        <body>
            <table>
                <!-- Table without tr -->
            </table>
            <table>
                 <!-- Table with empty tr -->
                 <tr></tr>
            </table>
            <table>
                <tr><th>Other</th><th>Data</th></tr>
                <tr><td>1</td><td>2</td></tr>
                <tr><td>1</td></tr> <!-- Mismatched cells -->
            </table>
            <table>
                <tr>
                    <th>Brand Name</th>
                    <th>Generic Name</th>
                    <th>Approval Date</th>
                </tr>
                <tr>
                    <td>Brand A</td>
                    <td>Generic A</td>
                    <td>2024-01-01</td>
                </tr>
                <tr>
                     <!-- Empty or mismatched row -->
                     <td>Just One</td>
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
        assert data[0]["brand_name_jp"] == "Brand A"

def test_approvals_source_complex_whitespace_headers() -> None:
    """Test headers with extra whitespace/newlines."""
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th> Approval   Date </th> <!-- Extra spaces -->
                    <th>Brand\nName</th>       <!-- Newline -->
                    <th>Generic Name</th>
                </tr>
                <tr>
                    <td>2024-01-01</td>
                    <td>Brand A</td>
                    <td>Generic A</td>
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
        # Current implementation of `get_text(strip=True)` removes leading/trailing but PRESERVES internal whitespace?
        # Actually bs4 `get_text(strip=True)` might collapse whitespace if we use separator?
        # Default `get_text(strip=True)`: "Approval   Date" -> "Approval Date"? No, it strips ends.
        # "Approval   Date" -> "Approval   Date".
        # My code uses `k in h`. "approval date" in "approval   date" is False.
        # So this test expects failure with current code if bs4 doesn't collapse.
        # bs4 get_text behavior: if there are tags inside, separator matters. If text node, it just returns text.
        # HTML collapse whitespace: BeautifulSoup parsing usually keeps text as is unless we normalize.

        # We expect this to capture data if code is robust, or we fix the code.
        # For now let's assert we get the data, and if it fails, we fix the code.
        assert len(data) == 1
        assert data[0]["brand_name_jp"] == "Brand A"
        assert data[0]["approval_date"] == "2024-01-01"

def test_approvals_source_multiple_valid_tables() -> None:
    """Test page with multiple valid tables (e.g. grouped by category)."""
    html_content = """
    <html>
        <body>
            <h1>New Drugs</h1>
            <table>
                <tr><th>Brand Name</th><th>Generic Name</th><th>Approval Date</th></tr>
                <tr><td>Brand A</td><td>Generic A</td><td>2024-01-01</td></tr>
            </table>
            <h1>Generics</h1>
            <table>
                <tr><th>Brand Name</th><th>Generic Name</th><th>Approval Date</th></tr>
                <tr><td>Brand B</td><td>Generic B</td><td>2024-02-01</td></tr>
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
        brands = sorted([d["brand_name_jp"] for d in data])
        assert brands == ["Brand A", "Brand B"]

def test_approvals_source_relative_urls() -> None:
    """Test URL joining with various relative paths."""
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>Brand Name</th><th>Generic Name</th><th>Approval Date</th><th>Review Report</th></tr>
                <tr>
                    <td>Brand A</td><td>Generic A</td><td>2024-01-01</td>
                    <td><a href="../reports/report_a.pdf">PDF</a></td>
                </tr>
                 <tr>
                    <td>Brand B</td><td>Generic B</td><td>2024-01-01</td>
                    <td><a href="/root/report_b.pdf">PDF</a></td>
                </tr>
            </table>
        </body>
    </html>
    """
    base_url = "https://www.pmda.go.jp/english/drugs/0001.html"

    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_get.return_value = mock_resp

        data = list(approvals_source(url=base_url))
        assert len(data) == 2

        # "https://www.pmda.go.jp/english/drugs/0001.html" + "../reports/report_a.pdf"
        # -> "https://www.pmda.go.jp/english/reports/report_a.pdf"
        assert data[0]["review_report_url"] == "https://www.pmda.go.jp/english/reports/report_a.pdf"

        # "https://www.pmda.go.jp/english/drugs/0001.html" + "/root/report_b.pdf"
        # -> "https://www.pmda.go.jp/root/report_b.pdf"
        assert data[1]["review_report_url"] == "https://www.pmda.go.jp/root/report_b.pdf"

def test_approvals_source_malformed_html() -> None:
    """Test handling of weird HTML structures."""
    # Table where 'td' count > 'th' count
    # Table where 'td' count < 'th' count
    html_content = """
    <html>
        <body>
            <table>
                <tr><th>Brand Name</th><th>Generic Name</th><th>Approval Date</th></tr>
                <!-- Too many cells -->
                <tr><td>Brand A</td><td>Generic A</td><td>2024-01-01</td><td>Extra</td></tr>
                <!-- Too few cells -->
                <tr><td>Brand B</td><td>Generic B</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        # Both rows should be skipped based on strict len check
        assert len(data) == 0
