# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import re
from collections.abc import Generator
from typing import Any, NamedTuple
from urllib.parse import urljoin

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
    Fetches the PMDA Approvals page, parses the relevant table, and yields
    structured row data.

    This logic is shared between the Approvals Source (metadata) and
    Review Reports Source (PDF downloads).
    """
    logger.debug(f"Parsing PMDA Approvals page: {url}")
    response = fetch_url(url)
    soup = get_soup(response)
    original_encoding = soup.original_encoding or response.encoding or "unknown"

    tables = soup.find_all("table")

    for table in tables:
        headers: list[str] = []
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

                record: dict[str, Any] = {}
                review_links: list[str] = []

                for idx, header in enumerate(headers):
                    cell: Tag = cells[idx]
                    cell_text = cell.get_text(strip=True)

                    record[header] = cell_text

                    # Extract Review Report URLs if this is the review column
                    if idx == review_col_idx:
                        a_tags = cell.find_all("a", href=True)
                        for a in a_tags:
                            href = a["href"]
                            full_url = urljoin(url, href)
                            review_links.append(full_url)

                # Basic validation: Must have "販売名" (Brand Name)
                if any("販売名" in k for k in record.keys()):
                    yield ParsedApprovalRow(
                        data=record,
                        review_report_links=review_links,
                        source_url=url,
                        original_encoding=original_encoding,
                    )
