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
from bs4 import Tag
from coreason_etl_pmda.config import settings
from coreason_etl_pmda.utils_logger import logger
from coreason_etl_pmda.utils_scraping import fetch_url, get_soup


@dlt.resource(name="bronze_approvals", write_disposition="append")  # type: ignore[misc]
def approvals_source(
    url: str = settings.URL_APPROVALS,
    application_type: str = "New Drug",
) -> dlt.sources.DltSource:
    """
    Ingests PMDA Approvals data (Japanese Source).
    """

    # Get state for incremental loading
    current_state = dlt.current.source_state()
    seen_ids = current_state.setdefault("seen_ids", [])
    seen_ids_set = set(seen_ids)

    logger.info(f"Scraping Approvals from {url} (Type: {application_type})")

    # Use shared scraping utility
    response = fetch_url(url)

    # Use shared BS4 helper
    soup = get_soup(response)
    original_encoding = soup.original_encoding or response.encoding or "unknown"

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
            text = re.sub(r"\s+", "", text)
            headers.append(text)

        # Heuristic to identify the correct table
        keywords = ["販売名", "一般的名称", "承認年月日", "承認番号"]
        matches = sum(1 for k in keywords if any(k in h for h in headers))

        if matches >= 2:
            logger.info(f"Found approval table with headers: {headers}")
            for tr in table.find_all("tr")[1:]:
                cells = tr.find_all("td")

                # Robustness: Skip rows that don't match header count (e.g. colspan/rowspan)
                if not cells or len(cells) != len(headers):
                    continue

                record = {}
                review_url = None

                for idx, header in enumerate(headers):
                    cell: Tag = cells[idx]
                    cell_text = cell.get_text(strip=True)

                    record[header] = cell_text

                    # Extract Review Report URL
                    if "報告書" in header or "概要" in header:
                        # Extract URL
                        # Look for 'a' tag. If multiple, take first?
                        a_tags = cell.find_all("a", href=True)
                        if a_tags:
                            # Take the first one as primary
                            href = a_tags[0]["href"]
                            review_url = urljoin(url, href)

                has_brand = any("販売名" in k for k in record.keys())

                if has_brand:
                    if review_url:
                        record["review_report_url"] = review_url

                    record["_source_url"] = url
                    record["application_type"] = application_type

                    # ID Generation
                    approval_no_key = next((k for k in record if "承認番号" in k), None)
                    approval_no = record.get(approval_no_key) if approval_no_key else None

                    if approval_no:
                        source_id = str(approval_no)
                    else:
                        brand_key = next((k for k in record if "販売名" in k), "")
                        date_key = next((k for k in record if "承認年月日" in k), "")
                        brand_val = record.get(brand_key, "")
                        date_val = record.get(date_key, "")
                        raw_str = f"{brand_val}|{date_val}"
                        source_id = hashlib.md5(raw_str.encode("utf-8")).hexdigest()

                    # Incremental Loading
                    if source_id in seen_ids_set:
                        continue

                    seen_ids_set.add(source_id)
                    new_ids.append(source_id)

                    yield {
                        "source_id": source_id,
                        "ingestion_ts": datetime.now(timezone.utc),
                        "original_encoding": original_encoding,
                        "raw_payload": record,
                    }

    if new_ids:
        seen_ids.extend(new_ids)
