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
from datetime import datetime, timezone

import dlt
from coreason_etl_pmda.config import settings
from coreason_etl_pmda.sources.common import yield_pmda_approval_rows
from coreason_etl_pmda.utils_logger import logger


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

    new_ids = []

    for row in yield_pmda_approval_rows(url):
        record = row.data
        review_links = row.review_report_links

        # Logic from original approvals.py:
        # Take the first review link as "review_report_url"
        if review_links:
            record["review_report_url"] = review_links[0]

        record["_source_url"] = row.source_url
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
            "original_encoding": row.original_encoding,
            "raw_payload": record,
        }

    if new_ids:
        seen_ids.extend(new_ids)
