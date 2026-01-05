# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import hashlib
import re
from datetime import datetime, timezone
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
    application_type: str = "New Drug",
) -> dlt.sources.DltSource:
    """
    Ingests PMDA Approvals data (Japanese Source).
    1. Scrapes the Japanese approvals page to find the table of drugs.
    2. Extracts metadata including `review_report_url` (審査報告書) from the HTML table.

    Args:
        url: The URL to scrape.
        application_type: "New Drug" or "Generic". Defaults to "New Drug" as the default URL is for P-Drugs.

    Refined Logic:
    - Fetch page (handling Shift-JIS or UTF-8).
    - Find tables.
    - Match headers using Japanese keywords.
    - Extract: 承認年月日 (Approval Date), 販売名 (Brand Name), 一般的名称 (Generic Name), 申請者氏名 (Applicant).
    - Yield dicts wrapped in Envelope Schema.
      Schema: source_id, ingestion_ts, raw_payload, original_encoding
    """

    # Get state for incremental loading
    current_state = dlt.current.source_state()
    # We track seen IDs to avoid duplicates.
    # Since we use write_disposition="append", we must filter manually.
    seen_ids = current_state.setdefault("seen_ids", [])
    # Convert list to set for O(1) lookup during processing
    seen_ids_set = set(seen_ids)

    logger.info(f"Scraping Approvals from {url} (Type: {application_type})")
    response = requests.get(url)
    response.raise_for_status()
    # PMDA often uses CP932/Shift-JIS, requests might autodetect or we force it if needed.
    # We'll rely on response.encoding or BeautifulSoup's detection.
    soup = BeautifulSoup(response.content, "html.parser")
    original_encoding = response.encoding or "unknown"

    tables = soup.find_all("table")

    new_ids = []

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
        # Added "承認番号" (Approval Number) to keywords as it's critical for source_id
        keywords = ["販売名", "一般的名称", "承認年月日", "承認番号"]
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

                    # Store raw Japanese key-value pairs
                    record[header] = cell_text

                    # Extract Review Report URL if present
                    if "報告書" in header or "概要" in header:  # 審査報告書 / 審査概要
                        # Extract URL
                        a_tag = cell.find("a", href=True)
                        if a_tag:
                            # Usually there are multiple links (Part 1, Part 2).
                            # We take the first one or a list?
                            # Spec says "review_report_url" (singular). We take the first.
                            review_url = urljoin(url, a_tag["href"])

                # Validation: Must have at least Brand Name to be useful
                # We check for "販売名" or "brand_name" key? No, Japanese key "販売名"
                # But headers might vary slightly (e.g. "販売名" vs "販売名(日本名)")
                # So we look for any key containing "販売名"
                has_brand = any("販売名" in k for k in record.keys())

                if has_brand:
                    # Enrich with metadata
                    if review_url:
                        record["review_report_url"] = review_url

                    record["_source_url"] = url
                    record["application_type"] = application_type

                    # Generate Source ID (Vendor Native ID)
                    # Priority: "承認番号" -> Hash(Brand + Date)
                    approval_no_key = next((k for k in record if "承認番号" in k), None)
                    approval_no = record.get(approval_no_key) if approval_no_key else None

                    if approval_no:
                        source_id = str(approval_no)
                    else:
                        # Fallback: Hash of Brand + Date
                        # We use Japanese keys
                        brand_key = next((k for k in record if "販売名" in k), "")
                        date_key = next((k for k in record if "承認年月日" in k), "")
                        brand_val = record.get(brand_key, "")
                        date_val = record.get(date_key, "")
                        raw_str = f"{brand_val}|{date_val}"
                        source_id = hashlib.md5(raw_str.encode("utf-8")).hexdigest()

                    # Incremental Loading Check
                    if source_id in seen_ids_set:
                        continue

                    # Mark as seen
                    seen_ids_set.add(source_id)
                    new_ids.append(source_id)

                    # Wrap in Envelope
                    yield {
                        "source_id": source_id,
                        "ingestion_ts": datetime.now(timezone.utc),
                        "original_encoding": original_encoding,
                        "raw_payload": record,
                    }

    # Update state with new IDs
    # Note: this grows indefinitely.
    # In a real high-volume scenario, we would use a sliding window or watermark date.
    # But for PMDA approvals which are relatively low volume (monthly updates), list of IDs is fine.
    # Also "High-Water Mark" usually implies Date-based.
    # But without reliable Date parsing in Source (we parse in Silver), ID tracking is safer for exact dedupe.
    if new_ids:
        seen_ids.extend(new_ids)
