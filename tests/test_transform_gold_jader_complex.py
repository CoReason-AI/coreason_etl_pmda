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
from coreason_etl_pmda.transformations.gold.transform_gold_jader import transform_jader_gold


def test_jader_mixed_bag_scenario() -> None:
    """
    Complex scenario covering multiple edge cases in one pass:
    - Case A: 1 Suspected, 1 Concomitant, 1 Reaction. (Expect 1 row).
    - Case B: 1 Suspected, No Reaction. (Expect 1 row, Reac=Null).
    - Case C: 0 Suspected, 2 Concomitant, 1 Reaction. (Expect 0 rows).
    - Case D: 2 Suspected, 2 Reactions. (Expect 4 rows).
    - Case E: 1 Suspected, Reac has ID but Null reaction text. (Expect 1 row, Reac=Null).
    """
    demo = pl.DataFrame(
        {
            "id": ["A", "B", "C", "D", "E"],
            "sex": ["M", "F", "M", "F", "M"],
            "age": ["10", "20", "30", "40", "50"],
            "reporting_year": [2021, 2021, 2021, 2021, 2021],
        }
    )

    drug = pl.DataFrame(
        {
            "id": [
                "A",
                "A",  # Case A: 1 Susp, 1 Conc
                "B",  # Case B: 1 Susp
                "C",
                "C",  # Case C: 2 Conc
                "D",
                "D",  # Case D: 2 Susp
                "E",  # Case E: 1 Susp
            ],
            "drug_name": [
                "Drug A_Susp",
                "Drug A_Conc",
                "Drug B_Susp",
                "Drug C_Conc1",
                "Drug C_Conc2",
                "Drug D_Susp1",
                "Drug D_Susp2",
                "Drug E_Susp",
            ],
            "characterization": [
                "Suspected",
                "Concomitant",
                "Suspected",
                "Concomitant",
                "Concomitant",
                "Suspected",
                "Suspected",
                "Suspected",
            ],
        }
    )

    reac = pl.DataFrame(
        {
            "id": [
                "A",  # Case A: 1 Reac
                # Case B: No Reac
                "C",  # Case C: 1 Reac
                "D",
                "D",  # Case D: 2 Reac
                "E",  # Case E: 1 Reac (Null text)
            ],
            "reaction": [
                "Reaction A",
                "Reaction C",
                "Reaction D1",
                "Reaction D2",
                None,
            ],
        }
    )

    result = transform_jader_gold(demo, drug, reac)

    # Expected Counts:
    # A: 1 (Susp) * 1 (Reac) = 1
    # B: 1 (Susp) * 1 (Null Reac row due to Left Join) = 1
    # C: 0 (Susp) -> 0
    # D: 2 (Susp) * 2 (Reac) = 4
    # E: 1 (Susp) * 1 (Reac with Null) = 1
    # Total: 7 rows
    assert len(result) == 7

    # Verify Case A
    case_a = result.filter(pl.col("case_id") == "A")
    assert len(case_a) == 1
    assert case_a["primary_suspect_drug"][0] == "Drug A_Susp"
    assert case_a["reaction_pt"][0] == "Reaction A"

    # Verify Case B (Missing Reac)
    case_b = result.filter(pl.col("case_id") == "B")
    assert len(case_b) == 1
    assert case_b["primary_suspect_drug"][0] == "Drug B_Susp"
    assert case_b["reaction_pt"][0] is None

    # Verify Case C (Dropped)
    assert len(result.filter(pl.col("case_id") == "C")) == 0

    # Verify Case D (Cartesian)
    case_d = result.filter(pl.col("case_id") == "D")
    assert len(case_d) == 4
    # Check distinct combinations
    combos = case_d.select(["primary_suspect_drug", "reaction_pt"]).unique()
    assert len(combos) == 4

    # Verify Case E (Null Reaction Text)
    case_e = result.filter(pl.col("case_id") == "E")
    assert len(case_e) == 1
    assert case_e["primary_suspect_drug"][0] == "Drug E_Susp"
    assert case_e["reaction_pt"][0] is None


def test_jader_japanese_id_join() -> None:
    """Tests joining logic with non-ASCII (Japanese) IDs."""
    # ID: "識別-1" (Identification-1)
    jp_id = "識別-1"

    demo = pl.DataFrame({"id": [jp_id], "sex": ["M"], "age": ["10"], "reporting_year": [2021]})
    drug = pl.DataFrame({"id": [jp_id], "drug_name": ["薬A"], "characterization": ["Suspected"]})
    reac = pl.DataFrame({"id": [jp_id], "reaction": ["反応A"]})

    result = transform_jader_gold(demo, drug, reac)

    assert len(result) == 1
    assert result["case_id"][0] == jp_id
    assert result["primary_suspect_drug"][0] == "薬A"
    assert result["reaction_pt"][0] == "反応A"


def test_jader_empty_inputs() -> None:
    """Tests behavior with empty input DataFrames (but with correct schema)."""
    # Create empty DFs with correct columns
    demo = pl.DataFrame(schema=["id", "sex", "age", "reporting_year"])
    drug = pl.DataFrame(schema=["id", "drug_name", "characterization"])
    reac = pl.DataFrame(schema=["id", "reaction"])

    result = transform_jader_gold(demo, drug, reac)

    assert len(result) == 0
    assert "case_id" in result.columns


def test_jader_large_one_to_many() -> None:
    """
    Performance/Correctness check for a single case with many drugs and reactions.
    1 Case, 10 Suspected Drugs, 10 Reactions -> 100 Rows.
    """
    case_id = "BIG_CASE"
    demo = pl.DataFrame({"id": [case_id], "sex": ["F"], "age": ["90"], "reporting_year": [2020]})

    # 10 Drugs
    drugs = [{"id": case_id, "drug_name": f"D{i}", "characterization": "Suspected"} for i in range(10)]
    drug_df = pl.DataFrame(drugs)

    # 10 Reactions
    reacs = [{"id": case_id, "reaction": f"R{i}"} for i in range(10)]
    reac_df = pl.DataFrame(reacs)

    result = transform_jader_gold(demo, drug_df, reac_df)

    assert len(result) == 100
    assert result["primary_suspect_drug"].n_unique() == 10
    assert result["reaction_pt"].n_unique() == 10


def test_jader_extra_columns_ignored() -> None:
    """Verify that extra columns in input do not crash logic and are ignored."""
    demo = pl.DataFrame({"id": ["1"], "sex": ["M"], "extra_col": ["ignore"]})
    drug = pl.DataFrame({"id": ["1"], "drug_name": ["D"], "characterization": ["Suspected"], "extra": ["X"]})
    reac = pl.DataFrame({"id": ["1"], "reaction": ["R"], "extra": ["Y"]})

    result = transform_jader_gold(demo, drug, reac)

    assert len(result) == 1
    assert "extra_col" not in result.columns
    assert "extra" not in result.columns


def test_jader_id_type_mismatch() -> None:
    """
    Tests joining when ID is integer in one table and string in another.
    This simulates incomplete type normalization in Bronze/Silver.
    Gold usually expects strict types, but Polars might error or fail to join.
    """
    # Demo ID is int, Drug ID is str
    demo = pl.DataFrame({"id": [1], "sex": ["M"]})
    drug = pl.DataFrame({"id": ["1"], "drug_name": ["D"], "characterization": ["Suspected"]})
    reac = pl.DataFrame({"id": ["1"], "reaction": ["R"]})

    # The join on column 'id' where types differ (Int64 vs String) usually throws an Error in Polars.
    # We want to verify it raises an error (strictness) or works if implicit cast (unlikely).
    try:
        transform_jader_gold(demo, drug, reac)
    except Exception as e:
        # Polars ComputeError or SchemaError
        assert "join" in str(e).lower() or "type" in str(e).lower()
        return

    # If it works, that's surprising but fine, but likely it won't join 1 with "1".
    # If no error, check if it joined.
    result = transform_jader_gold(demo, drug, reac)
    # If it didn't join, but "Zero Loss" preserves Drug ("1"), then we get 1 row with Null Demo.
    # Because Drug is "1" (String). Demo is 1 (Int).
    # Left Join Drug("1") -> Demo(1). No match.
    # Result: ID="1", sex=Null.
    assert len(result) == 1
    assert result["patient_sex"][0] is None


def test_jader_null_values_in_required_columns() -> None:
    """
    Tests behavior when required columns exist but contain Nulls (besides ID).
    e.g., Null drug_name, Null characterization (should be filtered if not Suspected).
    """
    # Null Characterization -> Should be filtered out?
    # Logic: filter(pl.col("characterization") == "Suspected")
    # Null == "Suspected" -> False/Null -> Filtered out.
    drug = pl.DataFrame({"id": ["1", "2"], "drug_name": ["D1", "D2"], "characterization": ["Suspected", None]})
    demo = pl.DataFrame({"id": ["1", "2"]})
    reac = pl.DataFrame({"id": ["1", "2"]})

    result = transform_jader_gold(demo, drug, reac)

    assert len(result) == 1
    assert result["case_id"][0] == "1"
