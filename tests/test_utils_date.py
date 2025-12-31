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
    # Kanji Gannen
    assert convert_japanese_date_to_iso("令和元年5月1日") == "2019-05-01"
    assert convert_japanese_date_to_iso("平成元年1月8日") == "1989-01-08"


def test_convert_japanese_date_standard() -> None:
    assert convert_japanese_date_to_iso("Reiwa 2.5.1") == "2020-05-01"
    assert convert_japanese_date_to_iso("Heisei 31.4.30") == "2019-04-30"  # Last day of Heisei
    assert convert_japanese_date_to_iso("Reiwa 1.5.1") == "2019-05-01"  # Reiwa 1 is same as Gannen


def test_convert_japanese_date_kanji_eras() -> None:
    assert convert_japanese_date_to_iso("令和2年5月1日") == "2020-05-01"
    assert convert_japanese_date_to_iso("平成31年4月30日") == "2019-04-30"
    assert convert_japanese_date_to_iso("昭和64年1月7日") == "1989-01-07"
    assert convert_japanese_date_to_iso("大正15年12月25日") == "1926-12-25"
    assert convert_japanese_date_to_iso("明治45年7月30日") == "1912-07-30"


def test_convert_japanese_date_short_names() -> None:
    assert convert_japanese_date_to_iso("R2.5.1") == "2020-05-01"
    assert convert_japanese_date_to_iso("H30.1.1") == "2018-01-01"
    # Added coverage for other eras
    assert convert_japanese_date_to_iso("S60.1.1") == "1985-01-01"
    assert convert_japanese_date_to_iso("T2.1.1") == "1913-01-01"
    assert convert_japanese_date_to_iso("M5.1.1") == "1872-01-01"


def test_convert_japanese_date_with_parenthesis() -> None:
    assert convert_japanese_date_to_iso("Reiwa 2 (2020) . 5 . 1") == "2020-05-01"
    assert convert_japanese_date_to_iso("令和2年(2020年)5月1日") == "2020-05-01"


def test_convert_japanese_date_kanji_separators() -> None:
    # Assuming the regex matches non-ascii separators implicitly by skipping them in re.findall(\d+)
    assert convert_japanese_date_to_iso("Reiwa 2年5月1日") == "2020-05-01"
    assert convert_japanese_date_to_iso("令和2年 5月 1日") == "2020-05-01"


def test_convert_japanese_date_defaults() -> None:
    # If month/day missing, default to Jan 1
    assert convert_japanese_date_to_iso("Reiwa 2") == "2020-01-01"
    assert convert_japanese_date_to_iso("令和2年") == "2020-01-01"
    assert convert_japanese_date_to_iso("Reiwa 2.5") == "2020-05-01"


def test_convert_japanese_date_invalid() -> None:
    assert convert_japanese_date_to_iso("Invalid") is None
    assert convert_japanese_date_to_iso("Reiwa 99.99.99") is None  # Invalid date
    assert convert_japanese_date_to_iso("") is None


def test_convert_japanese_date_full_width() -> None:
    # Full-width numbers
    assert convert_japanese_date_to_iso("令和２年５月１日") == "2020-05-01"
    assert convert_japanese_date_to_iso("令和２年１０月１０日") == "2020-10-10"


def test_convert_japanese_date_leap_year() -> None:
    # 2020 (Reiwa 2) was a leap year
    assert convert_japanese_date_to_iso("Reiwa 2.2.29") == "2020-02-29"
    # 2021 (Reiwa 3) was NOT a leap year
    assert convert_japanese_date_to_iso("Reiwa 3.2.29") is None


def test_convert_japanese_date_embedded_text() -> None:
    # Date embedded in other text
    assert convert_japanese_date_to_iso("Approved on Reiwa 2.5.1") == "2020-05-01"
    assert convert_japanese_date_to_iso("Approval Date: 令和2年5月1日") == "2020-05-01"


def test_convert_japanese_date_mixed_separators() -> None:
    assert convert_japanese_date_to_iso("Reiwa 2-5/1") == "2020-05-01"
    assert convert_japanese_date_to_iso("R2/5.1") == "2020-05-01"
