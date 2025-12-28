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
import pytest

from coreason_etl_pmda.transform_gold_jader import transform_jader_gold


def test_transform_jader_gold() -> None:
    demo = pl.DataFrame({"id": ["1", "2"], "sex": ["M", "F"], "age": ["20s", "30s"], "reporting_year": [2020, 2021]})

    drug = pl.DataFrame(
        {
            "id": ["1", "1", "2"],
            "drug_name": ["Drug A", "Drug B", "Drug C"],
            "characterization": ["Suspected", "Concomitant", "Suspected"],
        }
    )

    reac = pl.DataFrame({"id": ["1", "2"], "reaction": ["Headache", "Nausea"]})

    result = transform_jader_gold(demo, drug, reac)

    # Case 1: Has 2 drugs, but only A is suspected.
    # Join Demo(1) + Drug(A) + Reac(Headache) -> 1 row
    # Wait, Drug B is Concomitant, filtered out.

    # Case 2: Has Drug C suspected.
    # Join Demo(2) + Drug(C) + Reac(Nausea) -> 1 row

    assert len(result) == 2

    row1 = result.filter(pl.col("case_id") == "1").row(0, named=True)
    assert row1["primary_suspect_drug"] == "Drug A"
    assert row1["reaction_pt"] == "Headache"

    row2 = result.filter(pl.col("case_id") == "2").row(0, named=True)
    assert row2["primary_suspect_drug"] == "Drug C"


def test_transform_jader_gold_cartesian() -> None:
    # Test 1 case, 2 suspected drugs, 2 reactions -> 4 rows
    demo = pl.DataFrame({"id": ["1"], "sex": ["M"], "age": ["20"], "reporting_year": [2020]})
    drug = pl.DataFrame({"id": ["1", "1"], "drug_name": ["D1", "D2"], "characterization": ["Suspected", "Suspected"]})
    reac = pl.DataFrame({"id": ["1", "1"], "reaction": ["R1", "R2"]})

    result = transform_jader_gold(demo, drug, reac)
    assert len(result) == 4


def test_transform_jader_gold_missing_cols() -> None:
    demo = pl.DataFrame({"other": ["1"]})
    with pytest.raises(ValueError, match="demo_df missing key"):
        transform_jader_gold(demo, pl.DataFrame(), pl.DataFrame())


def test_transform_jader_gold_missing_char() -> None:
    demo = pl.DataFrame({"id": ["1"]})
    drug = pl.DataFrame({"id": ["1"]})  # Missing characterization
    reac = pl.DataFrame({"id": ["1"]})

    with pytest.raises(ValueError, match="missing 'characterization'"):
        transform_jader_gold(demo, drug, reac)
