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

# Regex to parse Japanese dates like "Reiwa 2 (2020) May 1" or "Reiwa Gannen"
# Typical formats seen in PMDA:
# - "Reiwa 2 (2020) . 5 . 1"
# - "Reiwa Gannen ..."
# - "R2.5.1" (though usually full kanji/names are used in docs)
# We will focus on the structure described in the problem statement: "Reiwa Gannen" or "Reiwa 2"
# and we need to output ISO 8601 YYYY-MM-DD.
# The input might be a full string or just the year part. The spec says "Convert Japanese Era (Reiwa 2) to ISO 8601 (2020-01-01)".
# If only year is provided, we might default to Jan 1? Or does it expect full date?
# "Reiwa 2 to ISO 8601 (2020-01-01)" suggests handling full dates, or defaulting.
# Let's assume we need to parse full dates where possible, but the Gannen logic is the critical part.


def convert_japanese_date_to_iso(date_str: str) -> str | None:
    """
    Parses a Japanese era date string and converts it to ISO 8601 format (YYYY-MM-DD).
    Handles 'Gannen' (first year) and standard years.
    Returns None if parsing fails.
    """
    if not date_str:
        return None

    # Normalization for parsing (strip whitespace)
    clean_str = date_str.strip()

    # Regex for "Era Year Month Day" pattern
    # Supports:
    # - Reiwa 2
    # - Reiwa Gannen
    # - R2
    # - H30
    # Separators can be ., -, /, or Japanese characters like 年, 月, 日

    # We'll start with a flexible regex.
    # Pattern: (Era)(Space?)(Year|Gannen)(Separator)(Month)(Separator)(Day)

    # Era mapping (English and Kanji, though spec uses English examples "Reiwa Gannen")
    # We should support Kanji if possible, but start with English as per spec examples.

    era_pattern = r"(?P<era>Reiwa|Heisei|Showa|Taisho|Meiji|R|H|S|T|M)"
    year_pattern = r"(?P<year>\d+|Gannen)"

    # Matches "Reiwa 2", "Reiwa Gannen", "R2"
    # Followed optionally by month and day
    # e.g. "Reiwa 2.5.1", "Reiwa 2 (2020) . 5 . 1"

    # A simple approach is to extract Era and Year first.
    match = re.search(f"{era_pattern}\\s*{year_pattern}", clean_str, re.IGNORECASE)

    if not match:
        # Try to see if it's already YYYY-MM-DD or similar? No, strict JP conversion here.
        return None

    era_str = match.group("era").capitalize()
    year_str = match.group("year")

    # Normalize Era
    if era_str.startswith("R"):
        era = "Reiwa"
    elif era_str.startswith("H"):
        era = "Heisei"
    elif era_str.startswith("S"):
        era = "Showa"
    elif era_str.startswith("T"):
        era = "Taisho"
    elif era_str.startswith("M"):
        era = "Meiji"
    else:  # pragma: no cover
        era = era_str  # Full name

    start_year = ERA_START_YEARS.get(era)
    # The regex guarantees 'era' is one of the keys in ERA_START_YEARS,
    # so start_year cannot be None.
    if start_year is None:  # pragma: no cover
        # Should be unreachable given the regex validation
        return None

    if year_str.lower() == "gannen":
        year_offset = 0  # Gannen is year 1
    else:
        year_offset = int(year_str) - 1

    gregorian_year = start_year + year_offset

    # Now look for Month and Day
    # We look for digits after the era/year block.
    # Common formats:
    # Reiwa 2 . 5 . 1
    # Reiwa 2 (2020) . 5 . 1
    # Reiwa 2年5月1日

    # Remove the Era/Year part we found to search for the rest
    remaining = clean_str[match.end() :]

    # If there is a parenthetical year (2020), ignore it
    remaining = re.sub(r"\(\d{4}\)", "", remaining)

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
    else:  # pragma: no cover
        # Should be covered by "if len >= 2" logic usually, but if no numbers found
        pass

    try:
        dt = datetime.date(gregorian_year, month, day)
        return dt.isoformat()
    except ValueError:
        return None  # pragma: no cover
