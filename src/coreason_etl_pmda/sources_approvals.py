# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from urllib.parse import urljoin
import re

import dlt
from bs4 import BeautifulSoup, Tag
from dlt.sources.helpers import requests

# URL for Approvals
# Approvals: https://www.pmda.go.jp/english/review-services/reviews/approved-information/drugs/0001.html
# This page likely contains links to monthly lists (Excel or PDF) or tables.
# The spec says: "Approvals: Scrape List -> Download Excel -> Load rows."
# High-Water Mark refresh strategy.


@dlt.resource(name="bronze_approvals", write_disposition="append")  # type: ignore[misc]
def approvals_source(
    url: str = "https://www.pmda.go.jp/english/review-services/reviews/approved-information/drugs/0001.html",
) -> dlt.sources.DltSource:
    """
    Ingests PMDA Approvals data.
    1. Scrapes the main approvals page to find the table of drugs.
    2. Extracts metadata including `review_report_url` from the HTML table.
    3. Finds links to Excel files for supplementary data?
       - Strategy Update: The HTML table is the primary source for the 'review_report_url'.
         If the table contains full data, we use it.
         If not, we might need to merge with Excel, but for this iteration, we focus on scraping the HTML
         to satisfy the "Review Reports (PDFs): Scraping: Yes... capture the review_report_url" requirement.
         We will yield records extracted from the HTML.

    Refined Logic:
    - Fetch page.
    - Find tables.
    - Iterate rows.
    - Extract: Approval Date, Brand Name, Generic Name, Applicant, Review Report URL (if present).
    - Yield dicts.
    """

    # Get state
    _ = dlt.current.source_state()
    # visited_urls might be relevant if we paginate or go into sub-pages.
    # For a single list page, we might just ingest all or check 'approval_date' vs high-water mark.
    # We will implement High-Water Mark based on 'approval_date' in the future (Silver/Gold dedupe),
    # or dlt incremental. Here we just scrape.

    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    # Locate the table.
    # Usually PMDA tables have class 'table-01' or similar, or just <table>.
    # We'll look for any table and try to identify columns.

    tables = soup.find_all("table")

    # We assume the table has headers that let us identify it, or we take the largest table.
    # Let's try to map headers.

    for table in tables:
        # Check headers
        headers = []
        header_row = table.find("tr")
        if not header_row:
            continue

        for th in header_row.find_all(["th", "td"]):
            text = th.get_text(strip=True).lower()
            # Normalize whitespace: replace newlines/tabs/multiple spaces with single space
            text = re.sub(r"\s+", " ", text)
            headers.append(text)

        # Heuristic to identify the correct table
        # Look for keywords: "brand name", "generic name", "approval date", "review report"
        keywords = ["brand name", "generic name", "approval date"]
        matches = sum(1 for k in keywords if any(k in h for h in headers))

        if matches >= 2:
            # Found likely table
            # Iterate rows (skip header)
            for tr in table.find_all("tr")[1:]:
                cells = tr.find_all("td")
                if not cells or len(cells) != len(headers):
                    continue

                record = {}
                review_url = None

                for idx, header in enumerate(headers):
                    cell: Tag = cells[idx]
                    cell_text = cell.get_text(strip=True)

                    # Map header to field
                    # We accept partial matches
                    if "approval date" in header:
                        record["approval_date"] = cell_text
                    elif "brand name" in header:
                        record["brand_name_jp"] = cell_text
                    elif "generic name" in header:
                        record["generic_name_jp"] = cell_text
                    elif "applicant" in header:
                        record["applicant_name_jp"] = cell_text
                    elif "indication" in header:
                        record["indication"] = cell_text
                    elif "review report" in header or "report" in header:
                        # Extract URL
                        a_tag = cell.find("a", href=True)
                        if a_tag:
                            review_url = urljoin(url, a_tag["href"])

                # If we found at least a brand name, yield
                if "brand_name_jp" in record:
                    record["review_report_url"] = review_url
                    record["_source_url"] = url

                    # We might also want to populate 'approval_id' if available in the table
                    # Usually it's not explicit in the English table, or maybe it is.
                    # We'll leave it as None if not found, Silver handles generation/extraction.

                    yield record

            # If we processed the main table, we might stop or continue to others?
            # PMDA might have multiple tables (one per month?). We continue.
