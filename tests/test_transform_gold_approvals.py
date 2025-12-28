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
import pytest

from coreason_etl_pmda.transform_gold_approvals import transform_approvals_gold


def test_transform_approvals_gold() -> None:
    # Input Data
    # Dates in Japanese Era
    # Text with half-width katakana
    input_df = pl.DataFrame(
        {
            "approval_id": ["123"],
            "brand_name_jp": [" ｱｽﾋﾟﾘﾝ "],  # Half-width -> Full-width
            "generic_name_jp": ["Aspirin"],
            "generic_name_en": ["Aspirin"],  # From Silver
            "approval_date": ["Reiwa 2.5.1"],  # -> 2020-05-01
            "application_type": ["New Drug"],
            "applicant_name_jp": ["Pharma Co"],
            "indication": ["Pain"],
            "review_report_url": ["http://url"],
        }
    )

    result = transform_approvals_gold(input_df)

    row = result.row(0, named=True)

    # Check ID generation
    # PMDA + 123 + 2020-05-01
    import hashlib

    expected_id = hashlib.sha256("PMDA1232020-05-01".encode()).hexdigest()
    assert row["coreason_id"] == expected_id

    # Check Date Normalization
    assert row["approval_date"] == "2020-05-01"

    # Check Text Normalization
    assert row["brand_name_jp"] == "アスピリン"

    # Check Schema
    assert "indication" in row
    assert row["indication"] == "Pain"


def test_transform_approvals_gold_missing_cols() -> None:
    # Missing required 'approval_id'
    df = pl.DataFrame({"brand_name_jp": ["A"]})
    with pytest.raises(ValueError, match="Missing required column"):
        transform_approvals_gold(df)


def test_transform_approvals_gold_defaults() -> None:
    # Missing optional columns like 'indication', 'applicant_name_jp'
    df = pl.DataFrame(
        {
            "approval_id": ["1"],
            "brand_name_jp": ["B"],
            "generic_name_jp": ["G"],
            "approval_date": ["R1.1.1"],
            "generic_name_en": ["G_EN"],
        }
    )

    result = transform_approvals_gold(df)
    row = result.row(0, named=True)

    assert row["indication"] is None
    assert row["applicant_name_jp"] is None
    assert row["coreason_id"] is not None
