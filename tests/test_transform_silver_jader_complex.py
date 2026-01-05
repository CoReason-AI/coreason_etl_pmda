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

from coreason_etl_pmda.silver.transform_silver_jader import (
    normalize_jader_demo,
    normalize_jader_drug,
)


def test_normalize_jader_whitespace_headers() -> None:
    # Header with spaces: "識別 番号" instead of "識別番号"
    df = pl.DataFrame(
        {
            "識別 番号": ["1"],  # Space inserted
            "性 別": ["男性"],  # Space inserted
            "年 齢": ["60歳代"],
            "報告年度": [2020],
        }
    )

    # This should succeed if we handle whitespace stripping in headers
    result = normalize_jader_demo(df)

    assert "id" in result.columns
    assert "sex" in result.columns
    assert "age" in result.columns
    assert result["id"][0] == "1"
    assert result["sex"][0] == "男性"


def test_normalize_jader_mixed_type_ids() -> None:
    # ID as integer
    df = pl.DataFrame(
        {
            "識別番号": [100, 200],  # Ints
            "性別": ["M", "F"],
            "年齢": ["20", "30"],
            "報告年度": [2020, 2020],
        }
    )

    result = normalize_jader_demo(df)

    # Should be cast to string
    assert result["id"].dtype == pl.String
    assert result["id"][0] == "100"


def test_normalize_jader_reporting_year_types() -> None:
    # Integer year
    df1 = pl.DataFrame({"識別番号": ["1"], "報告年度": [2020]})
    res1 = normalize_jader_demo(df1)
    # reporting_year should be Int64 as per Schema (int | None)
    assert res1["reporting_year"].dtype == pl.Int64
    assert res1["reporting_year"][0] == 2020

    # String year -> Coerced to Int64 by Pydantic
    df2 = pl.DataFrame({"識別番号": ["1"], "報告年度": ["2020"]})
    res2 = normalize_jader_demo(df2)
    assert res2["reporting_year"].dtype == pl.Int64
    assert res2["reporting_year"][0] == 2020


def test_normalize_jader_empty_dataframe() -> None:
    # Empty but with headers
    df = pl.DataFrame(schema=["識別番号", "性別", "年齢", "報告年度"])
    result = normalize_jader_demo(df)

    assert result.is_empty()
    assert "id" in result.columns
    assert "sex" in result.columns


def test_normalize_jader_extra_columns() -> None:
    # Extra columns should be removed by strict Pydantic validation (extra="ignore")
    df = pl.DataFrame(
        {
            "識別番号": ["1"],
            "性別": ["M"],
            "年齢": ["20"],
            "報告年度": [2020],
            "ExtraCol": ["Data"],
        }
    )

    result = normalize_jader_demo(df)
    assert "ExtraCol" not in result.columns


def test_normalize_jader_dirty_text_normalization() -> None:
    # Half-width kana, whitespace padding
    df = pl.DataFrame(
        {
            "識別番号": [" 1 "],  # Padded ID
            "性別": [" Ｍ "],  # Full width M with spaces
            "年齢": ["20"],
            "報告年度": [2020],
        }
    )

    result = normalize_jader_demo(df)

    # ID should be stripped?
    # Logic: normalize_text does strip().
    # ID is in cols_to_normalize.
    assert result["id"][0] == "1"
    # Sex should be stripped and NFKC
    assert result["sex"][0] == "M"  # Full width M might normalize to M if NFKC?
    # NFKC of "Ｍ" (U+FF2D) is "M" (U+004D).
    # Let's verify.


def test_normalize_jader_drug_characterization_variants() -> None:
    # Test values that are ALMOST correct but not exact?
    # Currently we use exact match replacement.
    # If space is present: " 被疑薬 "
    # normalize_text strips whitespace.
    # So " 被疑薬 " -> "被疑薬" -> "Suspected".
    # This assumes we normalize BEFORE replacing.
    # Let's check logic:
    # `transform_silver_jader.py`:
    # 1. Rename
    # 2. Normalize Text (cols: drug_name, characterization, ...)
    # 3. Replace values (normalize_jader_drug calls _normalize_common THEN replace).
    # So yes, it should handle padded values!

    df = pl.DataFrame(
        {
            "識別番号": ["1"],
            "医薬品（一般名）": ["Drug"],
            "被疑薬等区分": [" 被疑薬 "],  # Padded
        }
    )

    result = normalize_jader_drug(df)
    assert result["characterization"][0] == "Suspected"
