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
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import dlt
from coreason_etl_pmda.config import settings
from coreason_etl_pmda.utils_logger import logger
from coreason_etl_pmda.utils_scraping import fetch_url, get_soup


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
    response = fetch_url(url)
    soup = get_soup(response)
    original_encoding = response.encoding or "unknown"

    tables = soup.find_all("table")

    for table in tables:
        # Check headers to confirm it's the approvals table
        headers = []
        header_row = table.find("tr")
        if not header_row:
            continue

        for th in header_row.find_all(["th", "td"]):
            text = th.get_text(strip=True)
            text = re.sub(r"\s+", "", text)
            headers.append(text)

        keywords = ["販売名", "一般的名称", "審査報告書"]
        matches = sum(1 for k in keywords if any(k in h for h in headers))

        if matches >= 2:
            logger.info("Found table with review reports.")

            # Identify which column is "Review Report" (審査報告書) and "Brand Name" (販売名)
            try:
                report_col_idx = next(i for i, h in enumerate(headers) if "報告書" in h or "概要" in h)
                brand_col_idx = next(i for i, h in enumerate(headers) if "販売名" in h)
            except StopIteration:
                logger.warning("Could not find required columns in table.")
                continue

            for tr in table.find_all("tr")[1:]:
                cells = tr.find_all("td")
                if not cells or len(cells) < max(report_col_idx, brand_col_idx) + 1:
                    continue

                brand_cell = cells[brand_col_idx]
                brand_name = brand_cell.get_text(strip=True)

                report_cell = cells[report_col_idx]

                # Find all links in the report cell
                links = report_cell.find_all("a", href=True)

                if not links:
                    continue

                for i, link in enumerate(links):
                    pdf_url = urljoin(url, link["href"])
                    if not pdf_url.lower().endswith(".pdf"):
                        continue

                    if pdf_url in downloaded_ids:
                        continue

                    # Download PDF
                    try:
                        pdf_resp = fetch_url(pdf_url)

                        yield {
                            "source_id": pdf_url,
                            "ingestion_ts": datetime.now(timezone.utc),
                            "original_encoding": original_encoding,
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
