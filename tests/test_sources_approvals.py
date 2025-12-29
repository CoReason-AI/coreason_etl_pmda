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
