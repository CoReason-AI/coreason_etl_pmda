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
    Extended edge cases covering:
    - Standard years
    - Short Eras (R, H, S, T, M)
    - Full Eras (Reiwa, Heisei...)
    - Gannen (R1, Reiwa 1, Reiwa Gannen)
    - Full-width characters
    - Whitespace
    - Invalid inputs
    """

    # Define test cases
    # (Input, Expected Integer)
    cases = [
        ("2020", 2020),
        ("R1", 2019),
        ("H30", 2018),
        ("令和元年", 2019),
        (None, None),
        ("2021", 2021),
        ("InvalidYear", None),
        ("２０２０", 2020),  # Full-width digits
        ("Ｒ１", 2019),  # Full-width Era
        ("M1", 1868),  # Meiji 1
        ("T1", 1912),  # Taisho 1
        ("S1", 1926),  # Showa 1
        ("H1", 1989),  # Heisei 1
        ("Meiji 1", 1868),
        ("Taisho 1", 1912),
        ("Showa 1", 1926),
        ("Heisei 1", 1989),
        ("Reiwa 1", 2019),
        (" Reiwa 2 ", 2020),  # Whitespace
        ("R3", 2021),
        ("2020/01", None),  # Ambiguous date-like string (if split returns 2020, fine, but verify)
        # 2020/01 -> convert_japanese_date_to_iso might return None because it expects Era?
        # If it returns None, result is None.
    ]

    # Construct input lists
    # Use zero-padded IDs to ensure lexicographical sort matches numeric order
    ids = [f"{i:03d}" for i in range(len(cases))]
    years = [c[0] for c in cases]
    expected = [c[1] for c in cases]

    data_str = pl.DataFrame({"識別番号": ids, "性別": ["M"] * len(ids), "年齢": ["20"] * len(ids), "報告年度": years})

    res_str = normalize_jader_demo(data_str)
    rows = res_str.sort("id").to_dicts()

    # Assertions
    for i, exp in enumerate(expected):
        input_val = cases[i][0]
        actual = rows[i]["reporting_year"]
        assert actual == exp, f"Failed for input '{input_val}'. Expected {exp}, got {actual}"

    # Verify Case B: Already Integer column (Simulates clean CSV)
    data_int = pl.DataFrame(
        {
            "識別番号": ["999"],
            "性別": ["M"],
            "年齢": ["20"],
            "報告年度": [2022],  # Int type
        }
    )

    res_int = normalize_jader_demo(data_int)
    rows_int = res_int.sort("id").to_dicts()
    assert rows_int[0]["reporting_year"] == 2022
