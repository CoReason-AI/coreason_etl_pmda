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
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from requests.models import Response

from coreason_etl_pmda.transform_silver import (
    call_deepseek,
    jan_bridge_ai_fallback,
    jan_bridge_lookup,
    normalize_approvals,
)
from coreason_etl_pmda.utils_date import convert_japanese_date_to_iso
from coreason_etl_pmda.utils_text import normalize_text


@pytest.fixture  # type: ignore[misc]
def sample_approvals_raw() -> pl.DataFrame:
    """Returns a sample raw dataframe mimicking Bronze ingestion."""
    data = {
        "承認番号": ["12345", "67890", "   "],
        "承認年月日": ["Reiwa 2.5.1", "Heisei 30 (2018) . 1 . 1", None],
        "販売名": ["Brand A", "Brand B", "Brand C"],
        "一般的名称": ["Generic A", "Generic B", "Generic C"],
        "申請者氏名": ["Applicant A", "Applicant B", None],
        "薬効分類名": ["Indication A", "Indication B", None],
    }
    return pl.DataFrame(data)


@pytest.fixture  # type: ignore[misc]
def sample_jan_ref() -> pl.DataFrame:
    """Returns a sample JAN reference dataframe."""
    data = {
        "jan_name_jp": ["Generic A", "Generic X"],
        "jan_name_en": ["Generic A (JAN)", "Generic X (JAN)"],
        "inn_name_en": ["Generic A (INN)", None],
    }
    return pl.DataFrame(data)


def test_normalize_approvals_renaming(sample_approvals_raw: pl.DataFrame) -> None:
    """Tests that columns are correctly renamed."""
    df = normalize_approvals(sample_approvals_raw)
    expected_cols = [
        "approval_id",
        "approval_date",
        "brand_name_jp",
        "generic_name_jp",
        "applicant_name_jp",
        "indication",
        "coreason_id",
        "application_type",  # Added
    ]
    for col in expected_cols:
        assert col in df.columns


def test_normalize_approvals_date_conversion(sample_approvals_raw: pl.DataFrame) -> None:
    """Tests Japanese era date conversion."""
    df = normalize_approvals(sample_approvals_raw)

    # Reiwa 2.5.1 -> 2020-05-01
    assert df["approval_date"][0] == "2020-05-01"
    # Heisei 30 -> 1989 + 29 = 2018
    assert df["approval_date"][1] == "2018-01-01"
    # None -> None
    assert df["approval_date"][2] is None


def test_normalize_approvals_id_generation() -> None:
    """Tests deterministic coreason_id generation."""
    data = {
        "承認番号": ["123"],
        "承認年月日": ["Reiwa 2.1.1"],
    }
    df = pl.DataFrame(data)
    normalized = normalize_approvals(df)

    # ID = Hash("PMDA" + "123" + "2020-01-01")
    raw_str = "PMDA1232020-01-01"
    expected_hash = hashlib.sha256(raw_str.encode("utf-8")).hexdigest()

    assert normalized["coreason_id"][0] == expected_hash


def test_normalize_approvals_text_normalization() -> None:
    """Tests text normalization (NFKC, etc)."""
    # Half-width Katakana: ｱ (U+FF71) -> ア (U+30A2)
    data = {
        "販売名": ["ﾃｽﾄ"],  # "Test" in half-width
        "承認番号": ["123 "],  # Trim
        "application_type": ["New Drug"],
    }
    df = pl.DataFrame(data)
    normalized = normalize_approvals(df)

    assert normalized["brand_name_jp"][0] == "テスト"
    assert normalized["approval_id"][0] == "123"
    assert normalized["application_type"][0] == "New Drug"


def test_jan_bridge_lookup(sample_approvals_raw: pl.DataFrame, sample_jan_ref: pl.DataFrame) -> None:
    """Tests JAN Bridge Step 1: Lookup."""
    # Pre-normalize approvals
    silver_approvals = normalize_approvals(sample_approvals_raw)

    result = jan_bridge_lookup(silver_approvals, sample_jan_ref)

    # Generic A matches
    # jan_name_en="Generic A (JAN)", inn_name_en="Generic A (INN)" -> prefer INN
    assert result.filter(pl.col("generic_name_jp") == "Generic A")["generic_name_en"][0] == "Generic A (INN)"

    # Generic B (not in ref) -> Should be null
    assert result.filter(pl.col("generic_name_jp") == "Generic B")["generic_name_en"][0] is None


def test_jan_bridge_lookup_missing_columns() -> None:
    """Tests validation for missing columns."""
    df = pl.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="approvals_df must have generic_name_jp"):
        jan_bridge_lookup(df, df)

    # Test missing in jan_df
    df_approvals = pl.DataFrame({"generic_name_jp": ["A"]})
    df_jan = pl.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="jan_df must have jan_name_jp"):
        jan_bridge_lookup(df_approvals, df_jan)


@patch("coreason_etl_pmda.transform_silver.requests.post")
@patch.dict("os.environ", {"DEEPSEEK_API_KEY": "fake_key"})
def test_jan_bridge_ai_fallback_success(mock_post: MagicMock) -> None:
    """Tests AI fallback when lookup fails."""
    # Setup DF with missing generic_name_en
    df = pl.DataFrame(
        {
            "generic_name_jp": ["DrugX", "DrugY"],
            "brand_name_jp": ["BrandX", "BrandY"],
            "generic_name_en": [None, "DrugY (EN)"],
        }
    )

    # Mock Response
    mock_response = MagicMock()
    mock_response.json.return_value = {"choices": [{"message": {"content": "DrugX (INN)"}}]}
    mock_response.raise_for_status.return_value = None  # Success
    mock_post.return_value = mock_response

    result = jan_bridge_ai_fallback(df)

    # DrugX should be translated
    assert result.filter(pl.col("generic_name_jp") == "DrugX")["generic_name_en"][0] == "DrugX (INN)"
    assert result.filter(pl.col("generic_name_jp") == "DrugX")["_translation_status"][0] == "ai_translated"

    # DrugY should remain
    assert result.filter(pl.col("generic_name_jp") == "DrugY")["generic_name_en"][0] == "DrugY (EN)"
    assert result.filter(pl.col("generic_name_jp") == "DrugY")["_translation_status"][0] == "lookup_success"


@patch("coreason_etl_pmda.transform_silver.requests.post")
@patch.dict("os.environ", {"DEEPSEEK_API_KEY": "fake_key"})
def test_jan_bridge_ai_fallback_failure_exception(mock_post: MagicMock) -> None:
    """Tests AI fallback failure (API error exception)."""
    df = pl.DataFrame(
        {
            "generic_name_jp": ["DrugX"],
            "brand_name_jp": ["BrandX"],
            "generic_name_en": [None],
        }
    )

    mock_post.side_effect = Exception("API Error")

    result = jan_bridge_ai_fallback(df)

    assert result["generic_name_en"][0] is None
    assert result["_translation_status"][0] == "failed"


@patch("coreason_etl_pmda.transform_silver.requests.post")
@patch.dict("os.environ", {"DEEPSEEK_API_KEY": "fake_key"})
def test_jan_bridge_ai_fallback_failure_status_code(mock_post: MagicMock) -> None:
    """Tests AI fallback failure (HTTP 500 triggers raise_for_status)."""
    df = pl.DataFrame(
        {
            "generic_name_jp": ["DrugX"],
            "brand_name_jp": ["BrandX"],
            "generic_name_en": [None],
        }
    )

    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 500
    # requests.Response.raise_for_status raises HTTPError if status >= 400
    # We mock the method to raise
    from requests.exceptions import HTTPError

    mock_response.raise_for_status.side_effect = HTTPError("500 Error")

    mock_post.return_value = mock_response

    result = jan_bridge_ai_fallback(df)

    assert result["generic_name_en"][0] is None
    assert result["_translation_status"][0] == "failed"


@patch("coreason_etl_pmda.transform_silver.requests.post")
def test_call_deepseek_no_key(mock_post: MagicMock) -> None:
    """Tests call_deepseek returns None if no API key."""
    # Ensure env var is not set
    with patch.dict("os.environ", {}, clear=True):
        res = call_deepseek("A", "B")
        assert res is None
        mock_post.assert_not_called()


@patch("coreason_etl_pmda.transform_silver.requests.post")
@patch.dict("os.environ", {"DEEPSEEK_API_KEY": "fake_key"})
def test_call_deepseek_payload(mock_post: MagicMock) -> None:
    """Tests the payload structure sent to DeepSeek."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"choices": [{"message": {"content": "Res"}}]}
    mock_post.return_value = mock_response

    call_deepseek("GenJP", "BrandJP")

    args, kwargs = mock_post.call_args
    assert kwargs["json"]["model"] == "deepseek-chat"
    assert "Translate the Japanese pharmaceutical ingredient 'GenJP'" in kwargs["json"]["messages"][0]["content"]
    assert "Context: Brand is 'BrandJP'" in kwargs["json"]["messages"][0]["content"]


def test_jan_bridge_ai_fallback_no_missing() -> None:
    """Tests optimization when no translation needed."""
    df = pl.DataFrame({"generic_name_en": ["A", "B"]})
    result = jan_bridge_ai_fallback(df)
    # The function adds _translation_status even if skipped
    expected = df.with_columns(pl.lit("lookup_success").alias("_translation_status"))
    assert_frame_equal(expected, result)


@patch("coreason_etl_pmda.transform_silver.call_deepseek")
def test_jan_bridge_ai_fallback_concurrency_exception(mock_call: MagicMock) -> None:
    """Tests exception handling in concurrency loop."""
    # Force call_deepseek to raise Exception (not return None)
    mock_call.side_effect = Exception("Critical Failure")

    df = pl.DataFrame(
        {
            "generic_name_jp": ["DrugX"],
            "brand_name_jp": ["BrandX"],
            "generic_name_en": [None],
        }
    )

    result = jan_bridge_ai_fallback(df)

    assert result["generic_name_en"][0] is None
    assert result["_translation_status"][0] == "failed"


def test_jan_bridge_ai_fallback_empty_generic_jp() -> None:
    """Tests fallback when generic name is missing."""
    df = pl.DataFrame(
        {
            "generic_name_jp": [None, ""],
            "brand_name_jp": ["B1", "B2"],
            "generic_name_en": [None, None],
        }
    )
    result = jan_bridge_ai_fallback(df)
    assert result["_translation_status"][0] == "failed"
    assert result["_translation_status"][1] == "failed"


# --- Tests for Utils to improve coverage ---


def test_convert_japanese_date_iso_valid() -> None:
    """Tests date conversion happy paths."""
    assert convert_japanese_date_to_iso("Reiwa 2.5.1") == "2020-05-01"
    assert convert_japanese_date_to_iso("Reiwa Gannen 5.1") == "2019-05-01"
    assert convert_japanese_date_to_iso("H30.1.1") == "2018-01-01"
    assert convert_japanese_date_to_iso("S63.1.1") == "1988-01-01"
    assert convert_japanese_date_to_iso("T15.1.1") == "1926-01-01"
    assert convert_japanese_date_to_iso("M45.1.1") == "1912-01-01"


def test_convert_japanese_date_iso_invalid() -> None:
    """Tests date conversion failure modes."""
    assert convert_japanese_date_to_iso(None) is None  # type: ignore[arg-type]
    assert convert_japanese_date_to_iso("") is None
    assert convert_japanese_date_to_iso("Not a date") is None
    # No day/month
    # current implementation defaults to 1/1 if missing, let's verify
    assert convert_japanese_date_to_iso("Reiwa 2") == "2020-01-01"


def test_normalize_text_encodings_valid() -> None:
    """Tests text normalization with valid bytes."""
    # UTF-8
    assert normalize_text(b"test") == "test"
    # Shift-JIS (Katakana)
    sjis_bytes = "ﾃｽﾄ".encode("shift_jis")
    assert normalize_text(sjis_bytes) == "テスト"


def test_normalize_text_none() -> None:
    assert normalize_text(None) is None


def test_normalize_text_whitespace() -> None:
    assert normalize_text("  abc  ") == "abc"
