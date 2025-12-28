# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from unittest.mock import patch

import polars as pl
import pytest

from coreason_etl_pmda.transform_silver import jan_bridge_ai_fallback, jan_bridge_lookup


def test_jan_bridge_lookup() -> None:
    approvals = pl.DataFrame({"generic_name_jp": ["アスピリン", "不明"], "brand_name_jp": ["Brand A", "Brand B"]})

    jan_ref = pl.DataFrame({"jan_name_jp": ["アスピリン"], "jan_name_en": ["Aspirin JP"], "inn_name_en": ["Aspirin"]})

    result = jan_bridge_lookup(approvals, jan_ref)

    assert "generic_name_en" in result.columns

    # Match
    row1 = result.filter(pl.col("generic_name_jp") == "アスピリン").row(0, named=True)
    assert row1["generic_name_en"] == "Aspirin"  # prefers INN

    # Miss
    row2 = result.filter(pl.col("generic_name_jp") == "不明").row(0, named=True)
    assert row2["generic_name_en"] is None


def test_jan_bridge_lookup_fallback_jan() -> None:
    # If INN is missing, fallback to JAN
    approvals = pl.DataFrame(
        {
            "generic_name_jp": ["テスト"],
        }
    )

    jan_ref = pl.DataFrame({"jan_name_jp": ["テスト"], "jan_name_en": ["Test JAN"], "inn_name_en": [None]})

    result = jan_bridge_lookup(approvals, jan_ref)
    row = result.row(0, named=True)
    assert row["generic_name_en"] == "Test JAN"


def test_jan_bridge_ai_fallback() -> None:
    # Data with mixed status
    df = pl.DataFrame(
        {
            "generic_name_jp": ["アスピリン", "未知の薬"],
            "brand_name_jp": ["Brand A", "Brand X"],
            "generic_name_en": ["Aspirin", None],
        }
    )

    # Mock call_deepseek to return "Unknown Drug" for the missing one
    with patch("coreason_etl_pmda.transform_silver.call_deepseek") as mock_ai:
        mock_ai.return_value = "Unknown Drug"

        result = jan_bridge_ai_fallback(df)

        # Row 1: already had translation
        row1 = result.filter(pl.col("generic_name_jp") == "アスピリン").row(0, named=True)
        assert row1["generic_name_en"] == "Aspirin"
        assert row1["_translation_status"] == "lookup_success"

        # Row 2: AI translated
        row2 = result.filter(pl.col("generic_name_jp") == "未知の薬").row(0, named=True)
        assert row2["generic_name_en"] == "Unknown Drug"
        assert row2["_translation_status"] == "ai_translated"

        # Verify call
        mock_ai.assert_called_with("未知の薬", "Brand X")


def test_jan_bridge_ai_fallback_fail() -> None:
    # AI returns None
    df = pl.DataFrame({"generic_name_jp": ["未知の薬"], "brand_name_jp": ["Brand X"], "generic_name_en": [None]})

    with patch("coreason_etl_pmda.transform_silver.call_deepseek", return_value=None):
        result = jan_bridge_ai_fallback(df)

        row = result.row(0, named=True)
        assert row["generic_name_en"] is None
        assert row["_translation_status"] == "failed"


def test_jan_bridge_errors() -> None:
    with pytest.raises(ValueError):
        jan_bridge_lookup(pl.DataFrame(), pl.DataFrame())

    # Missing jan_name_jp in jan_df
    with pytest.raises(ValueError):
        jan_bridge_lookup(pl.DataFrame({"generic_name_jp": []}), pl.DataFrame())


def test_jan_bridge_ai_no_work() -> None:
    # All translated
    df = pl.DataFrame({"generic_name_en": ["Done"]})
    assert jan_bridge_ai_fallback(df) is df


def test_jan_bridge_ai_missing_generic_jp() -> None:
    # generic_name_jp is missing in row, should skip/return None
    # Wait, the inner function `translate` handles `if not generic_jp: return None`.
    # We need to trigger this path.
    df = pl.DataFrame({"generic_name_jp": [None], "brand_name_jp": ["Brand"], "generic_name_en": [None]})

    result = jan_bridge_ai_fallback(df)
    row = result.row(0, named=True)
    assert row["_translation_status"] == "failed"
    # And it didn't crash
