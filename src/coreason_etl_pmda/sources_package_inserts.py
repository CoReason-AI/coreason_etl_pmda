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
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import dlt
from bs4 import BeautifulSoup
from dlt.sources.helpers import requests

from coreason_etl_pmda.utils_logger import logger


@dlt.resource(name="bronze_package_inserts", write_disposition="append")  # type: ignore[misc]
def package_inserts_source(
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int = 7,
    base_url: str = "https://www.pmda.go.jp/PmdaSearch/iyakuSearch/",
) -> dlt.sources.DltSource:
    """
    Ingests Package Inserts (SGML/XML/HTML) from PMDA Search.

    Refresh Strategy: Delta.
    We search for updates within a date range using the 'Updated Date' (更新年月日) field.

    Args:
        start_date: YYYYMMDD string. Default: Today - lookback_days.
        end_date: YYYYMMDD string. Default: Today.
        lookback_days: Days to look back if start_date not provided.
        base_url: PMDA Search URL.
    """
    # Determine Date Range
    now = datetime.now(timezone.utc)
    if not end_date:
        end_date = now.strftime("%Y%m%d")

    if not start_date:
        start_dt = now - timedelta(days=lookback_days)
        start_date = start_dt.strftime("%Y%m%d")

    logger.info(f"Searching Package Inserts updated between {start_date} and {end_date}")

    # Prepare POST data
    payload = {
        "updateDocFrDt": start_date,
        "updateDocToDt": end_date,
        "relationDoc1check1": "on",  # Package Insert (添付文書)
        "ListRows": "100",  # Maximize rows
        "action:search": "検索",  # Submit button
    }

    # Perform Search
    logger.info(f"Posting search query to {base_url}")

    session = requests.Session()
    session.get(base_url)

    response = session.post(base_url, data=payload)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")

    if "該当するデータはありません" in response.text:
        logger.info("No updated package inserts found in range.")
        return

    page_count = 1
    while True:
        logger.info(f"Processing page {page_count}")
        rows = soup.find_all("tr")

        found_on_page = 0
        for row in rows:
            links = row.find_all("a", href=True)
            for link in links:
                href = link["href"]
                if "iyakuDetail" in href:
                    full_url = urljoin(base_url, href)

                    try:
                        yield from _process_detail_page(session, full_url)
                        found_on_page += 1
                        time.sleep(1.0)  # Rate limit
                    except Exception:
                        logger.exception(f"Failed to process detail page: {full_url}")

                    break

        logger.info(f"Found {found_on_page} items on page {page_count}")

        next_link = soup.find("a", string=lambda t: t and "次へ" in t)
        if next_link:
            next_url = urljoin(base_url, next_link["href"])
            logger.info(f"Moving to next page: {next_url}")
            response = session.get(next_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            page_count += 1
            time.sleep(1.0)
        else:
            break


def _process_detail_page(session: requests.Session, url: str) -> Generator[dict[str, Any], None, None]:
    """
    Fetches the drug detail page and extracts the Package Insert content (SGML/XML/HTML).
    """
    response = session.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    target_link = None

    # Priority: XML > SGML > HTML
    for ext in ["xml", "sgml", "html"]:
        # Fix B023: Bind ext=ext
        link = soup.find("a", href=lambda h, ext=ext: h and h.lower().endswith(f".{ext}"))
        if link:
            target_link = link["href"]
            break

    if not target_link:
        for keyword in ["XML", "SGML", "HTML", "添付文書"]:
            # Fix B023: Bind keyword=keyword
            link = soup.find("a", string=lambda t, keyword=keyword: t and keyword in t)
            if link:
                target_link = link["href"]
                break

    if target_link:
        full_target_url = urljoin(url, target_link)

        content_resp = session.get(full_target_url)
        content_resp.raise_for_status()

        encoding = content_resp.encoding or "utf-8"

        # Store raw content as bytes to avoid encoding issues
        content_bytes = content_resp.content

        yield {
            "source_id": full_target_url,
            "ingestion_ts": datetime.now(timezone.utc),
            "original_encoding": encoding,
            "raw_payload": {
                "content": content_bytes,  # DLT handles bytes
                "source_url": url,
                "content_url": full_target_url,
            },
        }
