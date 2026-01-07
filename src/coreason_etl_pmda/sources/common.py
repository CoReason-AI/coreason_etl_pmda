# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import io
import re
from collections.abc import Generator
from typing import Any, NamedTuple
from urllib.parse import urljoin

import openpyxl
from bs4 import Tag
from coreason_etl_pmda.utils_logger import logger
from coreason_etl_pmda.utils_scraping import fetch_url, get_soup


class ParsedApprovalRow(NamedTuple):
    """
    Structured data parsed from the PMDA Approvals table.
    """

    data: dict[str, str]  # Key-value pairs of the row content
    review_report_links: list[str]  # Full URLs to review reports (PDFs)
    source_url: str  # The page URL being scraped
    original_encoding: str  # Detected encoding of the page


def yield_pmda_approval_rows(url: str) -> Generator[ParsedApprovalRow, None, None]:
    """
    Fetches the PMDA Approvals page.
    1. Looks for an Excel file link. If found, ingests data from Excel.
    2. If no Excel file is found, falls back to parsing HTML tables.

    This logic is shared between the Approvals Source (metadata) and
    Review Reports Source (PDF downloads).
    """
    logger.debug(f"Parsing PMDA Approvals page: {url}")
    response = fetch_url(url)
    soup = get_soup(response)
    original_encoding = soup.original_encoding or response.encoding or "unknown"

    # 1. Search for Excel file
    excel_link = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith((".xlsx", ".xls")):
            # Heuristic: verify link text or context if needed?
            # For now, take the first Excel file found on the listing page
            # assuming it's the "List of Approved Products"
            excel_link = urljoin(url, href)
            logger.info(f"Found Excel list: {excel_link}")
            break

    if excel_link:
        try:
            logger.info(f"Downloading Excel file: {excel_link}")
            excel_resp = fetch_url(excel_link)
            # data_only=True ensures we get values, not formulas
            wb = openpyxl.load_workbook(io.BytesIO(excel_resp.content), data_only=True)
            ws = wb.active

            # Iterate rows
            # Assume first row is header
            rows = list(ws.iter_rows(values_only=False))
            rows_yielded = 0

            if not rows:
                logger.warning("Excel file is empty")
            else:
                raw_headers = [cell.value for cell in rows[0]]
                headers = []
                for h in raw_headers:
                    h_str = str(h) if h is not None else ""
                    # Normalize whitespace
                    h_norm = re.sub(r"\s+", "", h_str)
                    headers.append(h_norm)

                # Identify review report column index
                review_col_idx = -1
                try:
                    review_col_idx = next(i for i, h in enumerate(headers) if "報告書" in h or "概要" in h)
                except StopIteration:
                    pass

                for row in rows[1:]:
                    record: dict[str, Any] = {}
                    review_links: list[str] = []

                    for idx, cell in enumerate(row):
                        if idx >= len(headers):
                            continue

                        header = headers[idx]
                        val = cell.value
                        val_str = str(val) if val is not None else ""
                        record[header] = val_str

                        # Extract Hyperlink if review column
                        if idx == review_col_idx:
                            if cell.hyperlink:
                                # target might be relative or absolute
                                target = cell.hyperlink.target
                                if target:
                                    # Resolve relative to Excel URL? Or Page URL?
                                    # Usually relative to where the file is hosted?
                                    # Wait, Excel links are often absolute or relative to file.
                                    # If relative, we join with excel_link.
                                    full_link = urljoin(excel_link, target)
                                    review_links.append(full_link)

                            # Also check if the text itself is a URL (fallback)
                            if "http" in val_str:
                                review_links.append(val_str)

                    # Basic validation
                    if any("販売名" in k for k in record.keys()):
                        yield ParsedApprovalRow(
                            data=record,
                            review_report_links=review_links,
                            source_url=excel_link,
                            original_encoding="xlsx",
                        )
                        rows_yielded += 1

            # Only stop if we actually got data from Excel.
            # If excel file was empty or irrelevant (no matching rows), fall back to HTML.
            if rows_yielded > 0:
                logger.info(f"Ingested {rows_yielded} rows from Excel. Skipping HTML parsing.")
                return
            else:
                logger.warning(
                    f"Excel file {excel_link} contained no valid approval rows. Falling back to HTML scraping."
                )

        except Exception as e:
            logger.exception(f"Failed to process Excel file {excel_link}: {e}")
            # Fallback to HTML if Excel fails?
            logger.warning("Falling back to HTML scraping")

    # 2. HTML Scraping (Fallback)
    tables = soup.find_all("table")

    for table in tables:
        headers: list[str] = []  # type: ignore
        header_row = table.find("tr")
        if not header_row:
            continue

        for th in header_row.find_all(["th", "td"]):
            text = th.get_text(strip=True)
            # Normalize whitespace
            text = re.sub(r"\s+", "", text)
            headers.append(text)

        # Heuristic to identify the correct table
        # Combined keywords from approvals.py and review_reports.py
        keywords = ["販売名", "一般的名称", "承認年月日", "承認番号", "審査報告書"]
        matches = sum(1 for k in keywords if any(k in h for h in headers))

        if matches >= 2:
            logger.info(f"Found approval table with headers: {headers}")

            # Identify review report column index if present
            # Look for "審査報告書" (Review Report) or "概要" (Overview)
            review_col_idx = -1
            try:
                review_col_idx = next(i for i, h in enumerate(headers) if "報告書" in h or "概要" in h)
            except StopIteration:
                pass

            for tr in table.find_all("tr")[1:]:
                cells = tr.find_all("td")

                # Robustness: Skip rows that don't match header count (e.g. colspan/rowspan)
                if not cells or len(cells) != len(headers):
                    continue

                html_record: dict[str, Any] = {}
                html_review_links: list[str] = []

                for idx, header in enumerate(headers):
                    html_cell: Tag = cells[idx]
                    html_cell_text = html_cell.get_text(strip=True)

                    html_record[header] = html_cell_text

                    # Extract Review Report URLs if this is the review column
                    if idx == review_col_idx:
                        a_tags = html_cell.find_all("a", href=True)
                        for a in a_tags:
                            href = a["href"]
                            full_url = urljoin(url, href)
                            html_review_links.append(full_url)

                # Basic validation: Must have "販売名" (Brand Name)
                if any("販売名" in k for k in html_record.keys()):
                    yield ParsedApprovalRow(
                        data=html_record,
                        review_report_links=html_review_links,
                        source_url=url,
                        original_encoding=str(original_encoding),
                    )
