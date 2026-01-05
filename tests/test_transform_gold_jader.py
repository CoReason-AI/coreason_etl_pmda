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


def test_transform_jader_gold_basic() -> None:
    """Tests basic reconstruction logic."""
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

    # Logic Check:
    # Case 1: Drug A (Suspected), Drug B (Concomitant - dropped). Reaction: Headache.
    # -> 1 Row: (1, M, 20s, Drug A, Headache, 2020)
    # Case 2: Drug C (Suspected). Reaction: Nausea.
    # -> 1 Row: (2, F, 30s, Drug C, Nausea, 2021)

    assert len(result) == 2

    # Verify Case 1
    row1 = result.filter(pl.col("case_id") == "1").row(0, named=True)
    assert row1["primary_suspect_drug"] == "Drug A"
    assert row1["reaction_pt"] == "Headache"
    assert row1["patient_sex"] == "M"

    # Verify Case 2
    row2 = result.filter(pl.col("case_id") == "2").row(0, named=True)
    assert row2["primary_suspect_drug"] == "Drug C"


def test_transform_jader_gold_cartesian() -> None:
    """Tests Cartesian product: 1 Case x 2 Suspected Drugs x 2 Reactions = 4 Rows."""
    demo = pl.DataFrame({"id": ["1"], "sex": ["M"], "age": ["20"], "reporting_year": [2020]})
    drug = pl.DataFrame(
        {
            "id": ["1", "1"],
            "drug_name": ["D1", "D2"],
            "characterization": ["Suspected", "Suspected"],
        }
    )
    reac = pl.DataFrame({"id": ["1", "1"], "reaction": ["R1", "R2"]})

    result = transform_jader_gold(demo, drug, reac)
    assert len(result) == 4

    # Verify combinations exist
    combinations = result.select(["primary_suspect_drug", "reaction_pt"]).sort(["primary_suspect_drug", "reaction_pt"])
    expected = pl.DataFrame(
        {
            "primary_suspect_drug": ["D1", "D1", "D2", "D2"],
            "reaction_pt": ["R1", "R2", "R1", "R2"],
        }
    ).sort(["primary_suspect_drug", "reaction_pt"])

    # Cast to match types (String) if needed, usually polars infers strings.
    assert combinations.equals(expected)


def test_transform_jader_gold_filtering() -> None:
    """Tests that non-suspected drugs are strictly filtered out."""
    demo = pl.DataFrame({"id": ["1"]})
    drug = pl.DataFrame(
        {
            "id": ["1", "1"],
            "drug_name": ["Bad", "Good"],
            "characterization": ["Concomitant", "Suspected"],
        }
    )
    reac = pl.DataFrame({"id": ["1"], "reaction": ["R"]})

    result = transform_jader_gold(demo, drug, reac)
    assert len(result) == 1
    assert result["primary_suspect_drug"][0] == "Good"


def test_transform_jader_gold_dropped_cases() -> None:
    """
    Tests that cases without suspected drugs are dropped.
    Cases with suspected drug but no reaction should be KEPT.
    """
    demo = pl.DataFrame({"id": ["1", "2", "3"]})
    # Case 1: Has Suspected Drug, Has Reaction -> Keep
    # Case 2: Has Concomitant Drug only -> Drop (Filtered out before join)
    # Case 3: Has Suspected Drug, No Reaction -> Keep (Left Join)

    drug = pl.DataFrame(
        {
            "id": ["1", "2", "3"],
            "drug_name": ["D1", "D2", "D3"],
            "characterization": ["Suspected", "Concomitant", "Suspected"],
        }
    )

    reac = pl.DataFrame(
        {
            "id": ["1"],  # Only Case 1 has reaction
            "reaction": ["R1"],
        }
    )

    result = transform_jader_gold(demo, drug, reac)

    # Expecting Case 1 and Case 3
    assert len(result) == 2

    ids = result["case_id"].to_list()
    assert "1" in ids
    assert "3" in ids

    # Check Case 3 has None reaction
    case3 = result.filter(pl.col("case_id") == "3")
    assert case3["reaction_pt"][0] is None


def test_transform_jader_gold_missing_cols_validation() -> None:
    """Tests validation for missing keys."""
    # Missing key in demo
    with pytest.raises(ValueError, match="demo_df missing key"):
        transform_jader_gold(pl.DataFrame({"a": [1]}), pl.DataFrame(), pl.DataFrame())

    # Missing key in drug
    with pytest.raises(ValueError, match="drug_df missing key"):
        transform_jader_gold(pl.DataFrame({"id": [1]}), pl.DataFrame({"a": [1]}), pl.DataFrame())

    # Missing key in reac
    with pytest.raises(ValueError, match="reac_df missing key"):
        transform_jader_gold(
            pl.DataFrame({"id": [1]}),
            pl.DataFrame({"id": [1], "characterization": ["Suspected"]}),
            pl.DataFrame({"a": [1]}),
        )


def test_transform_jader_gold_missing_characterization() -> None:
    """Tests validation for missing characterization column."""
    demo = pl.DataFrame({"id": ["1"]})
    drug = pl.DataFrame({"id": ["1"]})  # Missing characterization
    reac = pl.DataFrame({"id": ["1"]})

    with pytest.raises(ValueError, match="missing 'characterization'"):
        transform_jader_gold(demo, drug, reac)
