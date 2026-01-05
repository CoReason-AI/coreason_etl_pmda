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
from bs4 import BeautifulSoup
from dlt.sources.helpers import requests

from coreason_etl_pmda.utils_logger import logger

# URL for JAN/INN data
# JAN/INN: https://www.nihs.go.jp/drug/jan_data_e.html


@dlt.resource(name="bronze_ref_jan_inn", write_disposition="replace")  # type: ignore[misc]
def jan_inn_source(url: str = "https://www.nihs.go.jp/drug/jan_data_e.html") -> dlt.sources.DltSource:
    """
    Ingests the NIHS "Japanese Accepted Names" Excel/CSV file.

    Logic:
    1. Fetch the provided URL.
    2. If Content-Type is HTML, parse it to find the latest Excel/CSV link.
    3. Download the file.
    4. Parse with Polars (Excel or CSV).
    5. Normalize headers to target schema (jan_name_jp, jan_name_en, inn_name_en).
    6. Yield rows.
    """
    logger.info(f"Accessing JAN/INN data source at {url}")
    response = requests.get(url)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "").lower()
    final_url = url
    file_content = response.content

    # If it's HTML, we need to find the file link
    if "text/html" in content_type:
        logger.info("URL points to HTML page. Searching for file link...")
        soup = BeautifulSoup(response.content, "html.parser")

        # heuristic: find links ending in .xlsx, .xls, .csv
        # Prioritize "JAN" in text
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
        file_resp = requests.get(final_url)
        file_resp.raise_for_status()
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
    # Mapping assumed from standard Japanese JAN files
    # Target: jan_name_jp, jan_name_en, inn_name_en
    header_mapping = {
        "JAN（日本名）": "jan_name_jp",
        "JAN(日本名)": "jan_name_jp",  # Handle full/half width variations
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

    # Handle duplicate target columns if source has multiple variants mapping to same target
    # We keep the first one encountered or prioritize?
    # Simple fix: if multiple source cols map to same target, we might have a collision in rename_dict values.
    # Polars rename requires unique output names if input names are different.
    # But here inputs are different keys in rename_dict.
    # We need to make sure we don't map two DIFFERENT columns to the SAME target name
    # in the same rename call if both exist.

    final_rename_dict = {}
    seen_targets = set()

    # Iterate over columns present in DF to determine what to rename
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
