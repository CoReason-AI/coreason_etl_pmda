# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import time
from datetime import datetime, timezone

import dlt
from coreason_etl_pmda.config import settings
from coreason_etl_pmda.sources.common import yield_pmda_approval_rows
from coreason_etl_pmda.utils_logger import logger
from coreason_etl_pmda.utils_scraping import fetch_url


@dlt.resource(name="bronze_review_reports", write_disposition="merge", primary_key="source_id")  # type: ignore[misc]
def review_reports_source(
    url: str = settings.URL_APPROVALS,
) -> dlt.sources.DltSource:
    """
    Ingests PMDA Review Reports (PDFs).
    """
    # Load state to skip existing
    current_state = dlt.current.source_state()
    downloaded_ids = current_state.setdefault("downloaded_ids", {})

    logger.info(f"Scraping Review Reports from {url}")

    for row in yield_pmda_approval_rows(url):
        record = row.data
        review_links = row.review_report_links

        brand_key = next((k for k in record if "販売名" in k), None)
        brand_name = record.get(brand_key, "") if brand_key else ""

        for i, pdf_url in enumerate(review_links):
            if not pdf_url.lower().endswith(".pdf"):
                continue

            if pdf_url in downloaded_ids:
                continue

            # Download PDF
            try:
                # fetch_url handles retries and delays
                pdf_resp = fetch_url(pdf_url)

                yield {
                    "source_id": pdf_url,
                    "ingestion_ts": datetime.now(timezone.utc),
                    "original_encoding": row.original_encoding,
                    "raw_payload": {
                        "content": pdf_resp.content,
                        "brand_name_jp": brand_name,
                        "part_index": i + 1,
                        "source_page_url": url,
                    },
                }

                # Mark as downloaded
                downloaded_ids[pdf_url] = int(time.time())

            except Exception:
                logger.exception(f"Failed to download PDF: {pdf_url}")
