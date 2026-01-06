# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda
from coreason_etl_pmda.utils_text import normalize_text


def test_normalize_text_half_width_katakana() -> None:
    # Half-width katakana "ｱｲｳｴｵ" -> Full-width "アイウエオ"
    half_width = "ｱｲｳｴｵ"
    expected = "アイウエオ"
    assert normalize_text(half_width) == expected


def test_normalize_text_encoding_utf8() -> None:
    text = "Hello World"
    assert normalize_text(text.encode("utf-8")) == "Hello World"


def test_normalize_text_encoding_cp932() -> None:
    # CP932/Shift-JIS specific characters
    # "日本語" in cp932
    text = "日本語"
    encoded = text.encode("cp932")
    assert normalize_text(encoded, encoding="cp932") == text
    # Should also auto-detect if primary encoding fails (e.g. if we passed utf-8)
    assert normalize_text(encoded, encoding="utf-8") == text


def test_normalize_text_encoding_euc_jp() -> None:
    text = "日本語"
    encoded = text.encode("euc-jp")
    assert normalize_text(encoded, encoding="euc-jp") == text
    # Auto-detect fallback
    assert normalize_text(encoded, encoding="utf-8") == text


def test_normalize_text_trimming() -> None:
    assert normalize_text("  Hello  ") == "Hello"


def test_normalize_text_none() -> None:
    assert normalize_text(None) is None


def test_normalize_text_fail_decode() -> None:
    # We can't easily patch built-ins like bytes.decode directly because they are immutable.
    # Instead, we'll subclass bytes and override decode, then pass that instance.

    from typing import Any

    class BadBytes(bytes):
        def decode(self, *args: Any, **kwargs: Any) -> str:
            raise UnicodeDecodeError("utf-8", b"", 0, 1, "mock failure")

    bad_bytes = BadBytes(b"some bytes")
    assert normalize_text(bad_bytes) is None


def test_normalize_text_string_input() -> None:
    assert normalize_text("Just a string") == "Just a string"
