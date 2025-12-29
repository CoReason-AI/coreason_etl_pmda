# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from coreason_etl_pmda.utils_date import convert_japanese_date_to_iso


def test_convert_japanese_date_gannen() -> None:
    assert convert_japanese_date_to_iso("Reiwa Gannen.5.1") == "2019-05-01"
    assert convert_japanese_date_to_iso("Reiwa Gannen . 5 . 1") == "2019-05-01"


def test_convert_japanese_date_standard() -> None:
    assert convert_japanese_date_to_iso("Reiwa 2.5.1") == "2020-05-01"
    assert convert_japanese_date_to_iso("Heisei 31.4.30") == "2019-04-30"  # Last day of Heisei
    assert convert_japanese_date_to_iso("Reiwa 1.5.1") == "2019-05-01"  # Reiwa 1 is same as Gannen


def test_convert_japanese_date_short_names() -> None:
    assert convert_japanese_date_to_iso("R2.5.1") == "2020-05-01"
    assert convert_japanese_date_to_iso("H30.1.1") == "2018-01-01"
    # Added coverage for other eras
    assert convert_japanese_date_to_iso("S60.1.1") == "1985-01-01"
    assert convert_japanese_date_to_iso("T2.1.1") == "1913-01-01"
    assert convert_japanese_date_to_iso("M5.1.1") == "1872-01-01"


def test_convert_japanese_date_with_parenthesis() -> None:
    assert convert_japanese_date_to_iso("Reiwa 2 (2020) . 5 . 1") == "2020-05-01"


def test_convert_japanese_date_kanji_separators() -> None:
    # Assuming the regex matches non-ascii separators implicitly by skipping them in re.findall(\d+)
    assert convert_japanese_date_to_iso("Reiwa 2年5月1日") == "2020-05-01"


def test_convert_japanese_date_defaults() -> None:
    # If month/day missing, default to Jan 1
    assert convert_japanese_date_to_iso("Reiwa 2") == "2020-01-01"
    assert convert_japanese_date_to_iso("Reiwa 2.5") == "2020-05-01"


def test_convert_japanese_date_invalid() -> None:
    assert convert_japanese_date_to_iso("Invalid") is None
    assert convert_japanese_date_to_iso("Reiwa 99.99.99") is None  # Invalid date
    assert convert_japanese_date_to_iso("") is None


def test_convert_japanese_date_idempotency() -> None:
    # Test that ISO dates are returned as is
    assert convert_japanese_date_to_iso("2020-05-01") == "2020-05-01"
    assert convert_japanese_date_to_iso("2019-12-31") == "2019-12-31"
