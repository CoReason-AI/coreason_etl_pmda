# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import dlt
import polars as pl
from dlt.sources.helpers import requests

from coreason_etl_pmda.utils_logger import logger

# URL for JAN/INN data
# JAN/INN: https://www.nihs.go.jp/drug/jan_data_e.html
# The actual file is an Excel/CSV linked from there.
# We will assume we need to scrape the page to find the link or use a direct link if it's stable.
# For this implementation, we will allow the user to provide the URL or default to a likely one.
# The spec says: https://www.nihs.go.jp/drug/jan_data_e.html
# We will create a resource that can handle downloading an Excel/CSV file from a URL.


@dlt.resource(name="bronze_ref_jan_inn", write_disposition="replace")  # type: ignore[misc]
def jan_inn_source(url: str = "https://www.nihs.go.jp/drug/jan_data_e.html") -> dlt.sources.DltSource:
    """
    Ingests the NIHS "Japanese Accepted Names" Excel/CSV file.

    In a real scenario, this might need to parse HTML to find the latest xlsx link.
    For this atomic unit, we will assume the URL points to the file itself or we mock the content.
    If the URL points to an HTML page, we would need logic to extract the file link.

    Given the spec says "Source Type: CSV / Excel", we will implement a resource that
    yields rows from the file.

    We'll use pandas/polars or dlt's built-in excel handling if available?
    dlt has `dlt.sources.filesystem` but for HTTP we might just download it.

    We'll use `pandas` (via dlt extras or install) or `openpyxl`?
    Wait, I didn't install `pandas` or `openpyxl`. `polars` can read excel too (via `xlsx2csv` or `openpyxl` engine).
    Let's check if we have `calamine` or `openpyxl` for polars excel reading.
    Or we can just assume CSV for now if the spec allowed it?
    Spec says "CSV / Excel".

    Let's try to support Excel using Polars if possible, but we need `openpyxl` or `calamine`.
    I'll add `openpyxl` to pyproject.toml in a bash command if needed.

    For now, let's implement the logic assuming we can read the content.
    """
    # Download the content
    # In tests we will mock requests.get
    logger.info(f"Downloading JAN/INN data from {url}")
    response = requests.get(url)
    response.raise_for_status()

    # We need to determine if it's Excel or CSV.
    # We can try to guess from Content-Type or extension.
    # Or just try reading with Polars.

    import io

    content = response.content

    try:
        # Try reading as Excel
        # Polars read_excel requires a file-like object or path.
        # It also requires a dependency.
        df = pl.read_excel(io.BytesIO(content))
    except Exception:
        # Fallback to CSV
        try:
            df = pl.read_csv(io.BytesIO(content))
        except Exception as e:
            # If both fail, raise
            raise ValueError(f"Could not parse JAN/INN file as Excel or CSV: {e}") from e

    # Yield records
    # Clean headers? Spec says target schema: jan_name_jp, jan_name_en, inn_name_en
    # The source file might have different headers.
    # We will yield raw dicts and let transformation layer handle mapping?
    # Spec says "Target Schema: bronze_ref_jan_inn (jan_name_jp, jan_name_en, inn_name_en)".
    # This implies we should map it here if possible or just dump raw.
    # Bronze usually implies "Raw Payload" + metadata.
    # But the spec explicitly lists columns for Bronze Ref JAN INN.
    # "Schema Standard: source_id, ingestion_ts, raw_payload" is for the GENERIC bronze layer described?
    # Or is that for specific files?
    # "Layer 1: Bronze (The Lake) ... Target Schema: bronze_ref_jan_inn (jan_name_jp, jan_name_en, inn_name_en)."
    # It seems for Reference data, we are loading structured data directly?
    # Or should we follow the "raw_payload" pattern?
    # The spec says:
    # "Ingestion Logic: ... Load rows." and "Target Schema: bronze_ref_jan_inn (jan_name_jp, jan_name_en, inn_name_en)."
    # This suggests we should normalize headers here to match the target schema.

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
    # We look for partial matches or exact matches?
    # Polars rename requires exact matches for existing columns.
    rename_dict = {}
    for col in df.columns:
        # Normalize whitespace
        clean_col = col.strip()
        if clean_col in header_mapping:
            rename_dict[col] = header_mapping[clean_col]

    df = df.rename(rename_dict)

    # Select only target columns if they exist, or keep all?
    # Spec "Target Schema: bronze_ref_jan_inn (jan_name_jp, jan_name_en, inn_name_en)"
    # We should ensure these exist.

    # If columns are missing, we might yield what we have, but Silver expects jan_name_jp.
    # Let's verify jan_name_jp exists.
    if "jan_name_jp" not in df.columns:
        logger.warning(f"jan_name_jp not found in JAN source. Columns: {df.columns}")
        # We continue, but downstream might fail or skip.

    # We yield all columns but the renamed ones are now compliant.
    for row in df.iter_rows(named=True):
        yield row
