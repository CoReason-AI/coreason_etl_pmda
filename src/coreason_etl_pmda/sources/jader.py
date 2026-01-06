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
import zipfile
from datetime import datetime, timezone
from urllib.parse import urljoin

import dlt
import polars as pl
from coreason_etl_pmda.config import settings
from coreason_etl_pmda.utils_logger import logger
from coreason_etl_pmda.utils_scraping import fetch_url, get_soup


@dlt.resource(name="bronze_jader", write_disposition="replace")  # type: ignore[misc]
def jader_source(
    url: str = settings.URL_JADER,
) -> dlt.sources.DltSource:
    """
    Ingests JADER data.
    """

    # 1. Scrape
    logger.info(f"Scraping JADER snapshot links from {url}")
    response = fetch_url(url)
    soup = get_soup(response)

    # Find zip links
    zip_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".zip"):
            full_url = urljoin(url, href)
            # Simple heuristic to avoid duplicate links or irrelevant ones
            if full_url not in zip_links:
                zip_links.append(full_url)

    logger.info(f"Found {len(zip_links)} JADER zip files")

    for zip_url in zip_links:
        try:
            logger.info(f"Processing JADER zip: {zip_url}")
            # Use fetch_url for zip download as well (handles retries/rate limit)
            resp = fetch_url(zip_url)

            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                for filename in z.namelist():
                    lower_name = filename.lower()
                    table_name = None

                    # Identify table type
                    if "demo" in lower_name and lower_name.endswith(".csv"):
                        table_name = "bronze_jader_demo"
                    elif "drug" in lower_name and lower_name.endswith(".csv"):
                        table_name = "bronze_jader_drug"
                    elif "reac" in lower_name and lower_name.endswith(".csv"):
                        table_name = "bronze_jader_reac"

                    if table_name:
                        with z.open(filename) as f:
                            content = f.read()

                            # Try decoding
                            # PMDA CSVs are often Shift-JIS / CP932
                            df = None
                            encodings = ["utf-8", "cp932", "shift_jis", "euc-jp"]

                            for enc in encodings:
                                try:
                                    # We use polars directly.
                                    # infer_schema_length=0 forces all columns to String
                                    df = pl.read_csv(io.BytesIO(content), encoding=enc, infer_schema_length=0)
                                    break
                                except Exception:
                                    continue

                            if df is None:
                                logger.error(f"Failed to decode {filename} in {zip_url}")
                                continue

                            ingestion_ts = datetime.now(timezone.utc)

                            # Vectorized addition of metadata columns
                            df = df.with_columns(
                                [
                                    pl.lit(filename).alias("_source_file"),
                                    pl.lit(zip_url).alias("_source_zip"),
                                    pl.lit(ingestion_ts).alias("_ingestion_ts"),
                                ]
                            )

                            # Yield Arrow Table wrapped in dlt marker
                            # Convert to Arrow Table
                            arrow_table = df.to_arrow()
                            yield dlt.mark.with_table_name(arrow_table, table_name)

        except Exception as e:
            logger.exception(f"Failed to process JADER zip {zip_url}: {e}")
