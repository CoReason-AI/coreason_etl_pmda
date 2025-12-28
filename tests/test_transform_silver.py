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

from coreason_etl_pmda.transform_silver import jan_bridge_ai_fallback, jan_bridge_lookup, normalize_approvals


# --- Tests for normalize_approvals ---
def test_normalize_approvals_basic() -> None:
    # Input DataFrame simulating Bronze Approvals (Raw Excel content)
    # Including: Japanese columns, Gannen dates, Mojibake/Half-width
    data = {
        "承認番号": ["12345", "67890"],
        "承認年月日": ["Reiwa 2.5.1", "H30.4.1"],
        "販売名": ["Brand A", "Brand B"],
        "一般的名称": ["Generic A", "Generic B"],
        "申請者氏名": ["Company A", "Company B"],
        "薬効分類名": ["Indication A", "Indication B"],
        "Extra": ["Keep", "Keep"],
    }
    df = pl.DataFrame(data)

    result = normalize_approvals(df)

    # Check renamed columns
    assert "approval_id" in result.columns
    assert "approval_date" in result.columns
    assert "brand_name_jp" in result.columns
    assert "generic_name_jp" in result.columns
    assert "applicant_name_jp" in result.columns
    assert "indication" in result.columns
    assert "Extra" in result.columns  # Ensure extra columns are kept

    # Check Date Normalization
    dates = result["approval_date"].to_list()
    assert dates[0] == "2020-05-01"  # Reiwa 2
    assert dates[1] == "2018-04-01"  # Heisei 30

    # Check coreason_id generation
    ids = result["coreason_id"].to_list()
    assert len(ids) == 2
    assert isinstance(ids[0], str)
    assert len(ids[0]) == 64  # SHA256 hex digest


def test_normalize_approvals_text_normalization() -> None:
    # Test half-width katakana and unicode issues
    data = {
        "承認番号": ["1"],
        "販売名": ["ｱｲｳｴｵ"],  # Half-width
        "一般的名称": ["  Spaced  "],  # Whitespace
        "承認年月日": ["R2.1.1"],
    }
    df = pl.DataFrame(data)

    result = normalize_approvals(df)
    row = result.row(0, named=True)

    assert row["brand_name_jp"] == "アイウエオ"
    assert row["generic_name_jp"] == "Spaced"  # Stripped


def test_normalize_approvals_missing_columns() -> None:
    # If source is missing some columns, they should be created as null
    data = {
        "承認番号": ["1"],
        # Missing others
    }
    df = pl.DataFrame(data)

    result = normalize_approvals(df)

    assert "brand_name_jp" in result.columns
    assert result["brand_name_jp"][0] is None
    assert "coreason_id" in result.columns


def test_normalize_approvals_id_consistency() -> None:
    # Deterministic ID check
    data = {
        "承認番号": ["100"],
        "承認年月日": ["Reiwa 2.1.1"],
    }
    df = pl.DataFrame(data)
    result = normalize_approvals(df)

    import hashlib

    expected_raw = "PMDA1002020-01-01"
    expected_hash = hashlib.sha256(expected_raw.encode("utf-8")).hexdigest()

    assert result["coreason_id"][0] == expected_hash


def test_normalize_approvals_id_robustness() -> None:
    # Ensure IDs are robust against whitespace/width variance in approval_id
    data = {
        "承認番号": ["  123  ", "１２３"],  # Whitespace, Full-width
        "承認年月日": ["R2.1.1", "R2.1.1"],
    }
    df = pl.DataFrame(data)
    result = normalize_approvals(df)

    id1 = result["coreason_id"][0]
    id2 = result["coreason_id"][1]

    # Both should normalize to "123" and produce same hash
    assert id1 == id2

    # Check that approval_id column itself is normalized
    assert result["approval_id"][0] == "123"
    assert result["approval_id"][1] == "123"


def test_normalize_approvals_date_complexity() -> None:
    # Test various complex date formats and invalid dates
    data = {
        "承認番号": ["1", "2", "3", "4"],
        "承認年月日": [
            "Reiwa Gannen.5.1",  # Gannen
            "R2/05/01",  # Slash
            "Reiwa 2 Year 5 Month 1 Day",  # Complex (Depends on regex robustness)
            "Reiwa 2.13.1",  # Invalid month
        ],
    }

    df = pl.DataFrame(data)
    result = normalize_approvals(df)

    dates = result["approval_date"].to_list()

    assert dates[0] == "2019-05-01"
    assert dates[1] == "2020-05-01"
    assert dates[2] == "2020-05-01"
    assert dates[3] is None  # Invalid date returns None


def test_normalize_approvals_mixed_input() -> None:
    # Mixed valid and invalid rows
    data = {
        "承認番号": ["1", "2"],
        "承認年月日": ["R2.1.1", None],  # Null date
        "販売名": ["Valid", None],  # Null text
    }
    df = pl.DataFrame(data)
    result = normalize_approvals(df)

    assert result["approval_date"][1] is None
    assert result["brand_name_jp"][1] is None

    # ID should handle None date
    # PMDA + 2 + ""
    import hashlib

    expected_raw = "PMDA2"
    expected_hash = hashlib.sha256(expected_raw.encode("utf-8")).hexdigest()
    assert result["coreason_id"][1] == expected_hash


def test_normalize_approvals_null_empty_behavior() -> None:
    # Empty strings vs Null
    data = {
        "承認番号": ["", None],
        "承認年月日": ["", None],
    }
    df = pl.DataFrame(data)
    result = normalize_approvals(df)

    # Empty string date -> None (utils_date handles empty string?)
    assert result["approval_date"][0] is None
    assert result["approval_date"][1] is None

    # ID for empty string ID vs None ID
    # Code: `str(sid) if sid is not None else ""`
    # If sid is "", `str("")` is "".
    # If sid is None, `""`.
    # So they produce same ID hash.
    assert result["coreason_id"][0] == result["coreason_id"][1]


# --- Existing Tests for JAN Bridge ---


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
            "brand_name_jp": ["Brand A", "Brand B"],
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
        mock_ai.assert_called_with("未知の薬", "Brand B")


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
    df = pl.DataFrame({"generic_name_jp": [None], "brand_name_jp": ["Brand"], "generic_name_en": [None]})

    result = jan_bridge_ai_fallback(df)
    row = result.row(0, named=True)
    assert row["_translation_status"] == "failed"
