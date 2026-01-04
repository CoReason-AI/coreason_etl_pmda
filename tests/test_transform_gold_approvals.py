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

import polars as pl
import pytest
from coreason_etl_pmda.transform_gold_approvals import transform_approvals_gold


def test_transform_approvals_gold() -> None:
    # Input Data (Simulating CLEAN Silver Output)
    # coreason_id should be pre-calculated
    # dates should be ISO
    # text should be normalized

    # coreason_id generation logic from Silver for testing consistency
    expected_id = hashlib.sha256("PMDA1232020-05-01".encode()).hexdigest()

    input_df = pl.DataFrame(
        {
            "coreason_id": [expected_id],
            "approval_id": ["123"],
            "brand_name_jp": ["アスピリン"],  # Already normalized
            "generic_name_jp": ["Aspirin"],
            "generic_name_en": ["Aspirin"],  # From Silver
            "approval_date": ["2020-05-01"],  # Already ISO
            "application_type": ["New Drug"],
            "applicant_name_jp": ["Pharma Co"],
            "indication": ["Pain"],
            "review_report_url": ["http://url"],
        }
    )

    result = transform_approvals_gold(input_df)

    row = result.row(0, named=True)

    # Check ID passed through
    assert row["coreason_id"] == expected_id

    # Check Columns passed through
    assert row["approval_date"] == "2020-05-01"
    assert row["brand_name_jp"] == "アスピリン"
    assert row["indication"] == "Pain"
    assert row["application_type"] == "New Drug"


def test_transform_approvals_gold_missing_cols() -> None:
    # Missing required column 'coreason_id' from Silver
    df = pl.DataFrame({"brand_name_jp": ["A"]})
    with pytest.raises(ValueError, match="Missing required column"):
        transform_approvals_gold(df)


def test_transform_approvals_gold_defaults() -> None:
    # Missing optional columns like 'indication', 'applicant_name_jp'
    # 'application_type' is required in Gold input validation because Silver provides it.

    # Let's provide required cols
    df = pl.DataFrame(
        {
            "coreason_id": ["hash123"],
            "approval_id": ["1"],
            "brand_name_jp": ["B"],
            "generic_name_jp": ["G"],
            "approval_date": ["2019-01-01"],
            "application_type": ["Generic"],
            "generic_name_en": ["G_EN"],
            # Missing: applicant_name_jp, indication, review_report_url
        }
    )

    result = transform_approvals_gold(df)
    row = result.row(0, named=True)

    # Should be created as None
    assert row["indication"] is None
    assert row["applicant_name_jp"] is None
    assert row["review_report_url"] is None

    # Required preserved
    assert row["coreason_id"] == "hash123"
    assert row["application_type"] == "Generic"
