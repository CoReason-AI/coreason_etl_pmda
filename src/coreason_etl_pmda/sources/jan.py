# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import io
from urllib.parse import urljoin

import dlt
import polars as pl

from coreason_etl_pmda.config import settings
from coreason_etl_pmda.utils_logger import logger
from coreason_etl_pmda.utils_scraping import fetch_url, get_soup


@dlt.resource(name="bronze_ref_jan_inn", write_disposition="replace")  # type: ignore[misc]
def jan_inn_source(url: str = settings.URL_JAN_INN) -> dlt.sources.DltSource:
    """
    Ingests the NIHS "Japanese Accepted Names" Excel/CSV file.
    """
    logger.info(f"Accessing JAN/INN data source at {url}")
    response = fetch_url(url)

    content_type = response.headers.get("Content-Type", "").lower()
    final_url = url
    file_content = response.content

    # If it's HTML, we need to find the file link
    if "text/html" in content_type:
        logger.info("URL points to HTML page. Searching for file link...")
        soup = get_soup(response)

        # heuristic: find links ending in .xlsx, .xls, .csv
        candidates = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            full_url = urljoin(url, href)
            lower_href = href.lower()

            if lower_href.endswith((".xlsx", ".xls", ".csv")):
                score = 0
                if "jan" in text.lower():
                    score += 10
                if "name" in text.lower():
                    score += 5
                if "list" in text.lower():
                    score += 5

                candidates.append((score, full_url))

        if not candidates:
            raise ValueError(f"No suitable Excel/CSV link found on {url}")

        # Sort by score descending
        candidates.sort(key=lambda x: x[0], reverse=True)
        best_link = candidates[0][1]
        logger.info(f"Found best file link: {best_link}")

        # Download the file
        final_url = best_link
        file_resp = fetch_url(final_url)
        file_content = file_resp.content

    # Parse content
    try:
        # Try reading as Excel
        df = pl.read_excel(io.BytesIO(file_content))
    except Exception:
        # Fallback to CSV
        try:
            df = pl.read_csv(io.BytesIO(file_content))
        except Exception as e:
            raise ValueError(f"Could not parse JAN/INN file from {final_url} as Excel or CSV: {e}") from e

    # Normalize Headers
    header_mapping = {
        "JAN（日本名）": "jan_name_jp",
        "JAN(日本名)": "jan_name_jp",
        "JAN（英名）": "jan_name_en",
        "JAN(英名)": "jan_name_en",
        "INN": "inn_name_en",
    }

    # Rename columns
    rename_dict = {}
    for col in df.columns:
        clean_col = col.strip()
        if clean_col in header_mapping:
            rename_dict[col] = header_mapping[clean_col]

    final_rename_dict = {}
    seen_targets = set()

    for col in df.columns:
        if col in rename_dict:
            target = rename_dict[col]
            if target not in seen_targets:
                final_rename_dict[col] = target
                seen_targets.add(target)
            else:
                logger.warning(f"Duplicate mapping for {target} found in column {col}. Ignoring this column.")

    df = df.rename(final_rename_dict)

    if "jan_name_jp" not in df.columns:
        logger.warning(f"jan_name_jp not found in JAN source. Columns: {df.columns}")

    # Yield rows
    for row in df.iter_rows(named=True):
        yield row
