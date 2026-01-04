# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import polars as pl


def transform_approvals_gold(approvals_df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms Silver approvals data into Gold `pmda_approvals` table.
    Gold layer strictly acts as a projection of clean Silver data.

    Schema:
      coreason_id (PK)
      approval_id (Source PK)
      application_type (Enum: "New Drug", "Generic")
      brand_name_jp (Source)
      generic_name_jp (Source - Normalized)
      generic_name_en (Target - Result of JAN Bridge)
      applicant_name_jp
      approval_date (ISO 8601)
      indication (Normalized text)
      review_report_url
    """

    # 1. Validation
    # We expect `coreason_id` and normalized fields to be present from Silver.
    required = [
        "coreason_id",
        "approval_id",
        "brand_name_jp",
        "generic_name_jp",
        "approval_date",
        "application_type",
    ]

    for c in required:
        if c not in approvals_df.columns:
            raise ValueError(f"Missing required column from Silver: {c}")

    # 2. Select final columns
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

    df = approvals_df

    # Ensure all exist (optional columns might be missing if Silver dropped them or didn't have them)
    # But strictly Silver should have schema.
    # We'll fill missing optional columns with None.
    for c in cols:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))

    return df.select(cols)
