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
from urllib.parse import urljoin

import dlt
import polars as pl
from bs4 import BeautifulSoup
from dlt.sources.helpers import requests

# URL for JADER
# JADER: https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/0008.html
# "Snapshot" refresh strategy. "Download Zip -> Extract CSVs".


@dlt.resource(name="bronze_jader", write_disposition="replace")  # type: ignore[misc]
def jader_source(
    url: str = "https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/0008.html",
) -> dlt.sources.DltSource:
    """
    Ingests JADER data.
    1. Scrapes the page to find the latest Zip file (or all zip files if multiple).
       Usually JADER is a single large snapshot or monthly releases.
       Spec says "Snapshot".
    2. Downloads Zip.
    3. Extracts CSVs (demo, drug, reac).
    4. Yields rows with table identifier.

    We need to yield to different tables?
    dlt resources can yield to different tables if we use `dlt.mark.with_table_name` or similar,
    but a single resource usually targets one table unless we yield `dlt.DynamicTable` or use transformer.
    However, "bronze_jader" might be the resource name, but we want 3 tables: `jader_demo`, `jader_drug`, `jader_reac`.

    Better approach:
    Create a source function that returns 3 resources.
    Or one resource that yields to dynamic tables.

    The spec says "Load to Bronze". "Target Schema: source_id, ingestion_ts, raw_payload" for Bronze generic?
    Or structured?
    "Ingestion Logic: JADER: Download Zip -> Extract CSVs (demo, drug, reac) -> Load to Bronze."

    If we want separate tables in Bronze, we should return a list of resources or use a generator that yields `dlt.DynamicTable`.
    Let's use a generator that yields data marked with table name.
    """

    # 1. Scrape
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    # Find zip links
    # Assuming we want the latest or all? "Snapshot" implies we might take the comprehensive file if it exists.
    # We'll look for .zip files.

    zip_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".zip"):
            zip_links.append(urljoin(url, href))

    # Process each zip
    for zip_url in zip_links:
        try:
            resp = requests.get(zip_url)
            resp.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                for filename in z.namelist():
                    # We look for specific CSVs: demo, drug, reac
                    # Files might be named like "demo.csv" or "jader_demo.csv".
                    lower_name = filename.lower()
                    table_name = None

                    if "demo" in lower_name and lower_name.endswith(".csv"):
                        table_name = "bronze_jader_demo"
                    elif "drug" in lower_name and lower_name.endswith(".csv"):
                        table_name = "bronze_jader_drug"
                    elif "reac" in lower_name and lower_name.endswith(".csv"):
                        table_name = "bronze_jader_reac"

                    if table_name:
                        with z.open(filename) as f:
                            # Read CSV with Polars
                            # encoding might be shift-jis
                            # We read as bytes first
                            content = f.read()

                            # Use Polars to read csv from bytes
                            # We try utf-8 first then cp932
                            try:
                                df = pl.read_csv(io.BytesIO(content))
                            except Exception:
                                try:
                                    df = pl.read_csv(io.BytesIO(content), encoding="cp932")
                                except Exception:  # pragma: no cover
                                    # Try Shift-JIS
                                    df = pl.read_csv(io.BytesIO(content), encoding="shift_jis")

                            # Yield rows marked with table name
                            for row in df.iter_rows(named=True):
                                record = row.copy()
                                record["_source_file"] = filename
                                record["_source_zip"] = zip_url
                                yield dlt.mark.with_table_name(record, table_name)

        except Exception as e:
            print(f"Failed to process {zip_url}: {e}")
            pass
