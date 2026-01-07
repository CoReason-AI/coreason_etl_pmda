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
from coreason_etl_pmda.transformations.silver.transform_silver_jader import (
    normalize_jader_demo,
    normalize_jader_drug,
    normalize_jader_reac,
)


def test_normalize_jader_demo() -> None:
    # Input with Japanese headers
    df = pl.DataFrame(
        {
            "識別番号": ["1", "2"],
            "性別": ["男性", "女性"],
            "年齢": ["60歳代", "70歳代"],
            "報告年度": [2020, 2021],
        }
    )

    result = normalize_jader_demo(df)

    # Check columns
    assert "id" in result.columns
    assert "sex" in result.columns
    assert "age" in result.columns
    assert "reporting_year" in result.columns

    # Check values (should be normalized string)
    assert result["id"][0] == "1"
    assert result["sex"][0] == "男性"  # We don't translate sex yet, just normalize text


def test_normalize_jader_drug() -> None:
    # Input with Japanese headers and Shift-JIS like values (though already decoded to string in Polars)
    # We test value mapping for characterization
    df = pl.DataFrame(
        {
            "識別番号": ["1", "2", "3"],
            "医薬品（一般名）": ["Drug A", "Drug B", "Drug C"],
            "被疑薬等区分": ["被疑薬", "併用薬", "Unknown"],
        }
    )

    result = normalize_jader_drug(df)

    assert "id" in result.columns
    assert "drug_name" in result.columns
    assert "characterization" in result.columns

    # Check mapping
    chars = result["characterization"]
    assert chars[0] == "Suspected"
    assert chars[1] == "Concomitant"
    assert chars[2] == "Unknown"  # Default behavior


def test_normalize_jader_reac() -> None:
    df = pl.DataFrame({"識別番号": ["1"], "有害事象": ["Reaction A"]})

    result = normalize_jader_reac(df)

    assert "id" in result.columns
    assert "reaction" in result.columns
    assert result["reaction"][0] == "Reaction A"


def test_normalize_jader_missing_columns_partial() -> None:
    # Missing '性別' (sex) -> Should create null column
    df = pl.DataFrame({"識別番号": ["1"]})

    result = normalize_jader_demo(df)

    assert "id" in result.columns
    assert "sex" in result.columns
    assert result["sex"][0] is None


def test_normalize_jader_missing_id_raises() -> None:
    # Missing '識別番号' -> Should raise ValueError
    df = pl.DataFrame({"性別": ["M"]})

    with pytest.raises(ValueError, match="Missing critical column 'id'"):
        normalize_jader_demo(df)


def test_normalize_jader_half_width_normalization() -> None:
    # Test NFKC normalization (Half-width Katakana -> Full-width)
    # ｱ -> ア
    df = pl.DataFrame({"識別番号": ["1"], "有害事象": ["ｱﾅﾌｨﾗｷｼｰ"]})

    result = normalize_jader_reac(df)
    assert result["reaction"][0] == "アナフィラキシー"


def test_normalize_jader_empty_dataframe() -> None:
    # Test handling of empty DataFrame
    # Must specify types, otherwise they are Null and string ops fail
    df = pl.DataFrame(schema={"識別番号": pl.String, "性別": pl.String})
    result = normalize_jader_demo(df)
    assert len(result) == 0
    # Check that schema matches output schema (roughly)
    assert "id" in result.columns
    assert "sex" in result.columns


def test_normalize_jader_demo_gannen_year() -> None:
    """Test normalization of Japanese Era years in reporting_year."""
    # "Reiwa 2" -> 2020
    # "Heisei 30" -> 2018
    # "R2" -> 2020
    df = pl.DataFrame(
        {
            "識別番号": ["1", "2", "3"],
            "報告年度": ["Reiwa 2", "Heisei 30", "R2"],
        }
    )

    result = normalize_jader_demo(df)
    years = result["reporting_year"]

    assert years[0] == 2020
    assert years[1] == 2018
    assert years[2] == 2020


def test_normalize_jader_demo_invalid_year() -> None:
    """Test normalization of invalid reporting_year."""
    df = pl.DataFrame(
        {
            "識別番号": ["1"],
            "報告年度": ["Unknown"],
        }
    )

    result = normalize_jader_demo(df)
    years = result["reporting_year"]

    assert years[0] is None


def test_normalize_jader_demo_string_year() -> None:
    """Test normalization of string integer reporting_year."""
    df = pl.DataFrame(
        {
            "識別番号": ["1"],
            "報告年度": ["2020"],
        }
    )

    result = normalize_jader_demo(df)
    years = result["reporting_year"]

    assert years[0] == 2020
