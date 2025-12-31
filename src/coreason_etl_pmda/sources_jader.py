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
from bs4 import BeautifulSoup
from dlt.sources.helpers import requests

from coreason_etl_pmda.utils_logger import logger

# URL for JADER
# JADER: https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/0008.html


@dlt.resource(name="bronze_jader", write_disposition="replace")  # type: ignore[misc]
def jader_source(
    url: str = "https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/0008.html",
) -> dlt.sources.DltSource:
    """
    Ingests JADER data.
    1. Scrapes the page to find Zip files.
    2. Downloads Zip.
    3. Extracts CSVs (demo, drug, reac).
    4. Yields rows with table identifier.

    Tables:
    - bronze_jader_demo
    - bronze_jader_drug
    - bronze_jader_reac
    """

    # 1. Scrape
    logger.info(f"Scraping JADER snapshot links from {url}")
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

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
            resp = requests.get(zip_url)
            resp.raise_for_status()

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
                                    # We use polars directly if possible, but for safety with encodings,
                                    # decoding to string first might be more reliable if polars fails.
                                    # However, Polars read_csv has encoding support.
                                    df = pl.read_csv(io.BytesIO(content), encoding=enc, infer_schema_length=0)
                                    break
                                except Exception:
                                    continue

                            if df is None:
                                logger.error(f"Failed to decode {filename} in {zip_url}")
                                continue

                            # Normalize headers?
                            # We keep raw headers in Bronze, but standardizing naming helps.
                            # For now, yield as is, but ensure string types for safety.
                            # infer_schema_length=0 forces all columns to String (mostly),
                            # which prevents schema mismatch errors during ingestion of raw data.

                            ingestion_ts = datetime.now(timezone.utc)

                            for row in df.iter_rows(named=True):
                                record = row.copy()
                                record["_source_file"] = filename
                                record["_source_zip"] = zip_url
                                record["_ingestion_ts"] = ingestion_ts
                                yield dlt.mark.with_table_name(record, table_name)

        except Exception as e:
            logger.exception(f"Failed to process JADER zip {zip_url}: {e}")
            # We don't stop the whole pipeline for one bad zip, but we log it.
            # If it's critical, we might want to raise.
            # Given "Completeness: 100% capture", a failure here is bad.
            # But iterating over all zips implies some might be historical/duplicate.
            # We'll continue but the logs will show errors.
