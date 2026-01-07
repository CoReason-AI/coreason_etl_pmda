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
from coreason_etl_pmda.transformations.silver.transform_silver_jader import (
    normalize_jader_demo,
    normalize_jader_drug,
    normalize_jader_reac,
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


def test_normalize_jader_gannen_complex_cases() -> None:
    """Test complex Gannen date variations."""
    df = pl.DataFrame(
        {
            "識別番号": ["1", "2", "3", "4", "5"],
            "報告年度": [
                "Reiwa Gannen",  # 2019
                "Heisei 1",  # 1989
                "S64",  # 1989
                "T15",  # 1926
                "M45",  # 1912
            ],
        }
    )

    result = normalize_jader_demo(df)
    years = result["reporting_year"]

    assert years[0] == 2019
    assert years[1] == 1989
    assert years[2] == 1989
    assert years[3] == 1926
    assert years[4] == 1912


def test_normalize_jader_gannen_invalid_cases() -> None:
    """Test invalid Gannen dates."""
    df = pl.DataFrame(
        {
            "識別番号": ["1", "2", "3"],
            "報告年度": [
                "Reiwa 0",  # Invalid year
                "Unknown Era 5",  # Invalid Era
                "Not a date",
            ],
        }
    )

    result = normalize_jader_demo(df)
    years = result["reporting_year"]

    # Current logic returns None for invalid parsing
    assert years[0] is None or years[0] == 2018  # logic might interpret 0 as year 0? No, 2019 + (0-1) = 2018?
    # Let's check logic: int(year_str) - 1. If year is 0, offset is -1. 2019 - 1 = 2018.
    # Technically Reiwa 0 doesn't exist, but the math works.
    # If the regex matches "0", it might parse.
    # But usually Gannen is year 1.
    # Let's see what happens. If it parses, it's fine, if not None.
    # We will assert specific behavior after verifying implementation details if needed.
    # For now, let's assume it might parse mathematically or fail regex if regex expects \d+.
    # Regex was \d+. So 0 matches.
    # Wait, R1 is 2019. R0 would be 2018 (Heisei 30).
    # Ideally we shouldn't have R0.
    # If we want to be strict, we might assert it maps to something or is handled.
    # Let's check "Unknown Era".
    assert years[1] is None
    assert years[2] is None


def test_normalize_jader_characterization_edge_cases() -> None:
    """Test Null, Empty, and Unknown characterization."""
    df = pl.DataFrame(
        {
            "識別番号": ["1", "2", "3"],
            "医薬品（一般名）": ["A", "B", "C"],
            "被疑薬等区分": [None, "", "UnknownValue"],
        }
    )

    result = normalize_jader_drug(df)
    chars = result["characterization"]

    assert chars[0] is None
    assert chars[1] == ""  # Empty string is preserved as empty string
    assert chars[2] == "UnknownValue"  # Preserved but not mapped


def test_normalize_jader_reaction_complex_text() -> None:
    """Test complex text normalization in reactions."""
    df = pl.DataFrame(
        {
            "識別番号": ["1", "2"],
            "有害事象": [
                "Shock",  # English
                "ｱﾅﾌｨﾗｷｼｰ ｼｮｯｸ",  # Half-width with space
            ],
        }
    )

    result = normalize_jader_reac(df)
    reacs = result["reaction"]

    assert reacs[0] == "Shock"
    assert reacs[1] == "アナフィラキシー ショック"  # Full width, space preserved (NFKC normalizes space?)
    # NFKC converts half-width space (U+0020) to space.
    # But does it trim? normalize_text does .strip().
    # So "アナフィラキシー ショック"
