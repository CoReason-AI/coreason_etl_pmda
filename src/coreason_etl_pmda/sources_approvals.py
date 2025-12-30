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
from urllib.parse import urljoin

import dlt
from bs4 import BeautifulSoup, Tag
from dlt.sources.helpers import requests

from coreason_etl_pmda.utils_logger import logger

# URL for Approvals (Japanese)
# Likely: https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0001.html
# We target the Japanese site to ensure we get `brand_name_jp` and `generic_name_jp` for the JAN Bridge.


@dlt.resource(name="bronze_approvals", write_disposition="append")  # type: ignore[misc]
def approvals_source(
    url: str = "https://www.pmda.go.jp/review-services/drug-reviews/review-information/p-drugs/0001.html",
) -> dlt.sources.DltSource:
    """
    Ingests PMDA Approvals data (Japanese Source).
    1. Scrapes the Japanese approvals page to find the table of drugs.
    2. Extracts metadata including `review_report_url` (審査報告書) from the HTML table.

    Refined Logic:
    - Fetch page (handling Shift-JIS or UTF-8).
    - Find tables.
    - Match headers using Japanese keywords.
    - Extract: 承認年月日 (Approval Date), 販売名 (Brand Name), 一般的名称 (Generic Name), 申請者氏名 (Applicant).
    - Yield dicts.
    """

    # Get state
    _ = dlt.current.source_state()

    logger.info(f"Scraping Approvals from {url}")
    response = requests.get(url)
    response.raise_for_status()
    # PMDA often uses CP932/Shift-JIS, requests might autodetect or we force it if needed.
    # We'll rely on response.encoding or BeautifulSoup's detection.
    soup = BeautifulSoup(response.content, "html.parser")

    tables = soup.find_all("table")

    for table in tables:
        # Check headers
        headers = []
        header_row = table.find("tr")
        if not header_row:
            continue

        for th in header_row.find_all(["th", "td"]):
            text = th.get_text(strip=True)
            # Normalize whitespace
            text = re.sub(r"\s+", "", text)  # Japanese text usually doesn't need spaces for keyword matching
            headers.append(text)

        # Heuristic to identify the correct table using Japanese keywords
        # Common headers: 承認年月日, 販売名, 一般的名称, 申請者氏名, 審査報告書
        keywords = ["販売名", "一般的名称", "承認年月日"]
        matches = sum(1 for k in keywords if any(k in h for h in headers))

        if matches >= 2:
            # Found likely table
            logger.info(f"Found approval table with headers: {headers}")
            for tr in table.find_all("tr")[1:]:
                cells = tr.find_all("td")
                if not cells or len(cells) != len(headers):
                    continue

                record = {}
                review_url = None

                for idx, header in enumerate(headers):
                    cell: Tag = cells[idx]
                    cell_text = cell.get_text(strip=True)

                    # Map header to field (Japanese)
                    if "承認年月日" in header:
                        record["approval_date"] = cell_text
                    elif "販売名" in header:
                        record["brand_name_jp"] = cell_text
                    elif "一般的名称" in header:
                        record["generic_name_jp"] = cell_text
                    elif "申請者氏名" in header:
                        record["applicant_name_jp"] = cell_text
                    elif "薬効" in header:  # 薬効分類名 (Indication class)
                        record["indication"] = cell_text
                    elif "報告書" in header or "概要" in header:  # 審査報告書 / 審査概要
                        # Extract URL
                        a_tag = cell.find("a", href=True)
                        if a_tag:
                            # Usually there are multiple links (Part 1, Part 2).
                            # We take the first one or a list?
                            # Spec says "review_report_url" (singular). We take the first.
                            review_url = urljoin(url, a_tag["href"])

                if "brand_name_jp" in record:
                    record["review_report_url"] = review_url
                    record["_source_url"] = url
                    yield record
