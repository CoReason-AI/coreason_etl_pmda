# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import datetime
import re

# Map Japanese era names to their start years
ERA_START_YEARS = {
    "Reiwa": 2019,
    "Heisei": 1989,
    "Showa": 1926,
    "Taisho": 1912,
    "Meiji": 1868,
}


def convert_japanese_date_to_iso(date_str: str) -> str | None:
    """
    Parses a Japanese era date string and converts it to ISO 8601 format (YYYY-MM-DD).
    Handles 'Gannen' (first year) and standard years.
    Supports both English (Reiwa) and Kanji (令和) formats.
    Returns None if parsing fails.
    """
    if not date_str:
        return None

    # Normalization for parsing (strip whitespace)
    clean_str = date_str.strip()

    # Regex for "Era Year Month Day" pattern
    # Supports:
    # - Reiwa 2, Reiwa Gannen, R2
    # - 令和2, 令和元年
    # Separators can be ., -, /, or Japanese characters like 年, 月, 日

    era_pattern = r"(?P<era>Reiwa|Heisei|Showa|Taisho|Meiji|R|H|S|T|M|令和|平成|昭和|大正|明治)"
    year_pattern = r"(?P<year>\d+|Gannen|元年)"

    # Match Era and Year
    match = re.search(f"{era_pattern}\\s*{year_pattern}", clean_str, re.IGNORECASE)

    if not match:
        return None

    era_str = match.group("era")
    year_str = match.group("year")

    # Normalize Era to English Key
    # Capitalize first if it's English to handle "reiwa" -> "Reiwa"
    if era_str[0].isascii():
        era_str = era_str.capitalize()

    era_norm = era_str
    if era_str.startswith("R") or era_str == "令和":
        era_norm = "Reiwa"
    elif era_str.startswith("H") or era_str == "平成":
        era_norm = "Heisei"
    elif era_str.startswith("S") or era_str == "昭和":
        era_norm = "Showa"
    elif era_str.startswith("T") or era_str == "大正":
        era_norm = "Taisho"
    elif era_str.startswith("M") or era_str == "明治":
        era_norm = "Meiji"

    start_year = ERA_START_YEARS.get(era_norm)
    if start_year is None:  # pragma: no cover
        return None

    # Handle Gannen
    if year_str.lower() in ["gannen", "元年"]:
        year_offset = 0  # Gannen is year 1
    else:
        year_offset = int(year_str) - 1

    gregorian_year = start_year + year_offset

    # Now look for Month and Day
    # Remove the Era/Year part we found
    remaining = clean_str[match.end() :]

    # If there is a parenthetical year (2020), ignore it
    remaining = re.sub(r"\(\d{4}[^\)]*\)", "", remaining)

    # Find next numbers
    numbers = re.findall(r"\d+", remaining)

    month = 1
    day = 1

    if len(numbers) >= 2:
        month = int(numbers[0])
        day = int(numbers[1])
    elif len(numbers) == 1:
        month = int(numbers[0])
        # Default day 1
    else:
        # If no numbers found, default to Jan 1
        pass

    try:
        dt = datetime.date(gregorian_year, month, day)
        return dt.isoformat()
    except ValueError:
        return None
