# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import dlt
import polars as pl
from bs4 import BeautifulSoup
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
    1. Scrapes the main approvals page to find Excel/Monthly links.
    2. Downloads the Excel files.
    3. Extracts rows.

    For this atomic unit, we will implement the scraping logic to find links and process one 'mock' link.
    We assume the links point to Excel files as per spec "Download Excel".

    We need to handle `High-Water Mark`?
    The spec says "Refresh Strategy: High-Water Mark".
    Usually this means we track the last processed file or date.
    dlt handles incremental loading if we define a primary key and merge, or cursor.
    For Bronze "append", we might just ingest everything and let Silver dedupe, or use dlt's state to skip visited URLs.

    Let's use dlt's state to track visited URLs.
    """
    # Get state
    current_state = dlt.current.source_state()
    visited_urls = current_state.get("visited_urls", {})

    # 1. Scrape Main Page
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    # Find links to Excel files.
    # We look for <a> tags ending in .xlsx or .xls
    # The page might link to sub-pages first (e.g. by Year/Month).
    # Given the complexity of scraping, we will implement a simplified version:
    # Find all links to .xlsx/.xls directly or assume simple structure.
    # If the user provided URL is the main list, we search recursively?
    # Spec says "Scrape List -> Download Excel".

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".xlsx") or href.lower().endswith(".xls"):
            # Resolve relative URL
            # dlt.sources.helpers.requests might not expose compat directly, import standard urllib
            from urllib.parse import urljoin

            full_url = urljoin(url, href)
            links.append(full_url)

    # If no direct excel links, maybe we are on the landing page and need to click years?
    # For this task, we assume we find at least one or the logic is to find them.
    # We will iterate found links.

    import io

    for file_url in links:
        if file_url in visited_urls:
            continue

        try:
            # Download Excel
            file_resp = requests.get(file_url)
            file_resp.raise_for_status()

            # Read Excel
            # Using Polars
            df = pl.read_excel(io.BytesIO(file_resp.content))

            # Yield rows
            # We add source metadata
            for row in df.iter_rows(named=True):
                # Add source_url to the row or let dlt handle it?
                # Spec says "Schema Standard: source_id: URL or File Name."
                # We can add a specialized field.
                record = row.copy()
                record["_source_url"] = file_url
                yield record

            # Mark as visited
            visited_urls[file_url] = True

        except Exception as e:
            # Log warning but continue?
            # Or raise?
            print(f"Failed to process {file_url}: {e}")
            # We don't mark as visited so we retry next time?
            pass

    # Save state
    current_state["visited_urls"] = visited_urls
