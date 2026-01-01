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
from typing import Any

import polars as pl

from coreason_etl_pmda.utils_date import convert_japanese_date_to_iso
from coreason_etl_pmda.utils_text import normalize_text


def transform_approvals_gold(approvals_df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms Silver approvals data into Gold `pmda_approvals` table.

    Schema:
      coreason_id (PK)
      approval_id (Source PK)
      application_type (Enum: "New Drug", "Generic")
      brand_name_jp (Source)
      generic_name_jp (Source - Normalized)
      generic_name_en (Target - Result of JAN Bridge) - passed in from Silver
      applicant_name_jp
      approval_date (ISO 8601)
      indication (Normalized text)
      review_report_url

    Assumes `approvals_df` already has `generic_name_en` populated (output of Silver JAN Bridge).
    """

    # 1. Normalize Text Fields
    # We apply normalization to JP fields: brand_name_jp, generic_name_jp, applicant_name_jp, indication

    # Helper for applying normalization
    # Since our normalize_text is python, we map elements.
    # Note: For large datasets, a native polars expression or rust plugin is better,
    # but here we stick to atomic python util.

    def norm_str(s: str | None) -> str | None:
        return normalize_text(s) if s else None  # pragma: no cover

    def norm_date(s: str | None) -> str | None:
        return convert_japanese_date_to_iso(s) if s else None  # pragma: no cover

    # We assume column mapping from source names to target names happens here or before?
    # Source names from scraped Excel might vary. We'll assume standardized names for this function
    # or mappable via config.
    # For this atomic unit, we assume the input DF has columns:
    # `approval_id`, `brand_name_jp`, `generic_name_jp`, `applicant_name_jp`,
    # `approval_date`, `indication`, `review_report_url`, `generic_name_en`, `application_type`

    # Check required columns
    required = ["approval_id", "brand_name_jp", "generic_name_jp", "approval_date"]
    # application_type is expected to be present from Silver.
    # However, for safety, if missing, we can default it or fail.
    # Spec says "application_type (Enum: 'New Drug', 'Generic')".
    # We should ensure it's present.
    if "application_type" not in approvals_df.columns:
        # We could default, but Silver should have provided it.
        pass

    for c in required:
        if c not in approvals_df.columns:
            # Maybe strict check or optional? Spec implies these are core.
            # But let's allow flexibility if columns missing?
            # No, Gold schema is strict.
            raise ValueError(f"Missing required column: {c}")

    # Apply Normalization
    df = approvals_df.with_columns(
        [
            pl.col("brand_name_jp").map_elements(norm_str, return_dtype=pl.String).alias("brand_name_jp"),
            pl.col("generic_name_jp").map_elements(norm_str, return_dtype=pl.String).alias("generic_name_jp"),
            pl.col("applicant_name_jp").map_elements(norm_str, return_dtype=pl.String).alias("applicant_name_jp")
            if "applicant_name_jp" in approvals_df.columns
            else pl.lit(None).alias("applicant_name_jp"),
            pl.col("indication").map_elements(norm_str, return_dtype=pl.String).alias("indication")
            if "indication" in approvals_df.columns
            else pl.lit(None).alias("indication"),
            pl.col("approval_date").map_elements(norm_date, return_dtype=pl.String).alias("approval_date"),
        ]
    )

    # 2. Generate coreason_id
    # Logic: Hash("PMDA" + source_id + approval_date)
    # source_id is approval_id.

    def generate_id(struct: dict[str, Any]) -> str:
        sid = struct.get("approval_id") or ""
        date = struct.get("approval_date") or ""
        raw = f"PMDA{sid}{date}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    df = df.with_columns(
        pl.struct(["approval_id", "approval_date"])
        .map_elements(generate_id, return_dtype=pl.String)
        .alias("coreason_id")
    )

    # 3. Select final columns
    cols = [
        "coreason_id",
        "approval_id",
        "application_type",
        "brand_name_jp",
        "generic_name_jp",
        "generic_name_en",
        "applicant_name_jp",
        "approval_date",
        "indication",
        "review_report_url",
    ]

    # Ensure all exist
    for c in cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))

    return df.select(cols)
