import io
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import openpyxl
from coreason_etl_pmda.sources.common import yield_pmda_approval_rows


class TestSourcesApprovalsExcel(unittest.TestCase):
    def _create_mock_excel(
        self,
        headers: list[str],
        rows: list[list[Any]],
        hyperlink_target: str | None = None,
    ) -> bytes:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(headers)
        for row_data in rows:
            ws.append(row_data)

        # Add hyperlink to last cell of last row if requested
        if hyperlink_target and rows:
            # cell index (row, col) - 1-based
            # last row is len(rows) + 1 (header)
            r_idx = len(rows) + 1
            # last col is len(headers)
            c_idx = len(headers)
            cell = ws.cell(row=r_idx, column=c_idx)
            cell.hyperlink = hyperlink_target

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        return output.read()

    @patch("coreason_etl_pmda.sources.common.fetch_url")
    def test_excel_ingestion_success(self, mock_fetch: MagicMock) -> None:
        """
        Test that we correctly detect and parse an Excel file when present.
        """
        base_url = "http://example.com/page"
        excel_url = "http://example.com/list.xlsx"

        # Mock 1: The HTML page containing the link to Excel
        mock_html_resp = MagicMock()
        html_text = f'<html><body><a href="{excel_url}">Download List</a></body></html>'
        mock_html_resp.text = html_text
        mock_html_resp.content = html_text.encode("utf-8")  # BeautifulSoup needs bytes
        mock_html_resp.encoding = "utf-8"

        # Mock 2: The Excel file content
        headers = ["販売名", "承認年月日", "審査報告書"]
        rows = [["Drug A", "2020-01-01", "Report"]]
        excel_content = self._create_mock_excel(headers, rows, hyperlink_target="report.pdf")

        mock_excel_resp = MagicMock()
        mock_excel_resp.content = excel_content

        # Side effect sequence
        mock_fetch.side_effect = [mock_html_resp, mock_excel_resp]

        results = list(yield_pmda_approval_rows(base_url))

        self.assertEqual(len(results), 1)
        row = results[0]
        self.assertEqual(row.data["販売名"], "Drug A")
        self.assertEqual(row.source_url, excel_url)
        # Check if hyperlink was extracted
        self.assertIn("http://example.com/report.pdf", row.review_report_links)

    @patch("coreason_etl_pmda.sources.common.fetch_url")
    def test_fallback_on_empty_excel(self, mock_fetch: MagicMock) -> None:
        """
        Test fallback to HTML if Excel is found but empty/invalid.
        """
        base_url = "http://example.com/page"
        # excel_url variable removed as it was unused in logic/assertions

        # Mock 1: HTML page with Excel link AND a table
        # We need at least 2 keywords in headers for the table heuristic to match
        html_content = """
        <html><body>
            <a href="list.xlsx">List</a>
            <table>
                <tr><th>販売名</th><th>承認年月日</th></tr>
                <tr><td>Drug B (HTML)</td><td>2020-01-01</td></tr>
            </table>
        </body></html>
        """
        mock_html_resp = MagicMock()
        mock_html_resp.text = html_content
        mock_html_resp.content = html_content.encode("utf-8")
        mock_html_resp.encoding = "utf-8"

        # Mock 2: Empty Excel file (only headers or empty)
        excel_content = self._create_mock_excel([], [])
        mock_excel_resp = MagicMock()
        mock_excel_resp.content = excel_content

        mock_fetch.side_effect = [mock_html_resp, mock_excel_resp]

        results = list(yield_pmda_approval_rows(base_url))

        # Should fall back to HTML and find Drug B
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].data["販売名"], "Drug B (HTML)")
        self.assertEqual(results[0].source_url, base_url)

    @patch("coreason_etl_pmda.sources.common.fetch_url")
    def test_fallback_on_irrelevant_excel(self, mock_fetch: MagicMock) -> None:
        """
        Test fallback if Excel exists but doesn't contain approval data (e.g. diff headers).
        """
        base_url = "http://example.com/page"

        # Mock 1: HTML
        html_content = """
        <html><body>
            <a href="other.xlsx">Other List</a>
            <table>
                <tr><th>販売名</th><th>承認年月日</th></tr>
                <tr><td>Drug C (HTML)</td><td>2020-02-02</td></tr>
            </table>
        </body></html>
        """
        mock_html_resp = MagicMock()
        mock_html_resp.text = html_content
        mock_html_resp.content = html_content.encode("utf-8")
        mock_html_resp.encoding = "utf-8"

        # Mock 2: Excel with random data (no "販売名")
        headers = ["Column A", "Column B"]
        rows = [["Val 1", "Val 2"]]
        excel_content = self._create_mock_excel(headers, rows)
        mock_excel_resp = MagicMock()
        mock_excel_resp.content = excel_content

        mock_fetch.side_effect = [mock_html_resp, mock_excel_resp]

        results = list(yield_pmda_approval_rows(base_url))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].data["販売名"], "Drug C (HTML)")
