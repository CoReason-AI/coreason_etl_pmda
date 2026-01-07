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
from coreason_etl_pmda.transformations.silver.transform_silver_jader import normalize_jader_demo


def test_jader_gannen_year_normalization() -> None:
    """
    Test that JADER reporting_year handles Japanese Era years (Gannen).
    """
    # Create DataFrame with various year formats
    # 2020: Standard integer string
    # R1: Reiwa 1 (2019)
    # H30: Heisei 30 (2018)
    # 令和元年: Reiwa Gannen (2019)
    # empty/null: Should stay null
    # "2021": String Int -> Should be Int
    # "Invalid": Should be None

    # Case A: Mixed/String column (Simulates CSV with "R1")
    data_str = pl.DataFrame(
        {
            "識別番号": ["1", "2", "3", "4", "5", "6", "7"],
            "性別": ["M", "F", "M", "F", "M", "F", "M"],
            "年齢": ["20", "30", "40", "50", "60", "70", "80"],
            # All strings because of "R1"
            "報告年度": ["2020", "R1", "H30", "令和元年", None, "2021", "InvalidYear"],
        }
    )

    res_str = normalize_jader_demo(data_str)
    rows = res_str.sort("id").to_dicts()

    assert rows[0]["reporting_year"] == 2020
    assert rows[1]["reporting_year"] == 2019
    assert rows[2]["reporting_year"] == 2018
    assert rows[3]["reporting_year"] == 2019
    assert rows[4]["reporting_year"] is None
    assert rows[5]["reporting_year"] == 2021
    assert rows[6]["reporting_year"] is None

    # Case B: Already Integer column (Simulates clean CSV)
    data_int = pl.DataFrame(
        {
            "識別番号": ["8"],
            "性別": ["M"],
            "年齢": ["20"],
            "報告年度": [2022],  # Int type
        }
    )

    res_int = normalize_jader_demo(data_int)
    rows_int = res_int.sort("id").to_dicts()
    assert rows_int[0]["reporting_year"] == 2022
