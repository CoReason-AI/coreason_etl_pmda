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
from coreason_etl_pmda.transform_gold_approvals import transform_approvals_gold
from coreason_etl_pmda.transform_gold_jader import transform_jader_gold
from coreason_etl_pmda.utils_date import convert_japanese_date_to_iso
from coreason_etl_pmda.utils_text import normalize_text


def test_date_edge_cases() -> None:
    # Invalid dates
    assert convert_japanese_date_to_iso("Reiwa 2.2.30") is None  # Feb 30 invalid
    assert convert_japanese_date_to_iso("Reiwa 2.13.1") is None  # Month 13
    assert convert_japanese_date_to_iso("Reiwa 2.0.1") is None  # Month 0

    # Weird separators
    assert convert_japanese_date_to_iso("Reiwa 2..5..1") == "2020-05-01"
    assert convert_japanese_date_to_iso("Reiwa 2//5//1") == "2020-05-01"

    # Full width numbers in date string
    assert convert_japanese_date_to_iso("Reiwa ２.５.１") == "2020-05-01"


def test_text_edge_cases() -> None:
    # Text with mixed valid/invalid utf-8 sequences
    assert normalize_text("") == ""

    # Case: Byte sequence that is valid in CP932 but not UTF-8
    cp932_bytes = "日本語".encode("cp932")
    assert normalize_text(cp932_bytes) == "日本語"


def test_jader_orphan_records() -> None:
    # Demo exists, Drug exists but not Suspected, Reac exists.
    demo = pl.DataFrame({"id": ["1"]})
    drug = pl.DataFrame({"id": ["1"], "drug_name": ["D1"], "characterization": ["Concomitant"]})
    reac = pl.DataFrame({"id": ["1"], "reaction": ["R1"]})

    # Should result in empty because Inner Join on Suspected Drugs
    result = transform_jader_gold(demo, drug, reac)
    assert len(result) == 0

    # Demo exists, Drug Suspected, but No Reaction
    drug = pl.DataFrame({"id": ["1"], "drug_name": ["D1"], "characterization": ["Suspected"]})
    reac = pl.DataFrame({"id": ["2"], "reaction": ["R1"]})  # ID mismatch

    result = transform_jader_gold(demo, drug, reac)
    assert len(result) == 0


def test_jader_case_sensitivity() -> None:
    # "suspected" vs "Suspected"
    # Logic uses `filter(pl.col("characterization") == "Suspected")`
    demo = pl.DataFrame({"id": ["1"]})
    drug = pl.DataFrame({"id": ["1"], "drug_name": ["D1"], "characterization": ["suspected"]})
    reac = pl.DataFrame({"id": ["1"], "reaction": ["R1"]})

    result = transform_jader_gold(demo, drug, reac)
    assert len(result) == 0  # Should fail if strict "Suspected"


def test_approvals_gold_id_stability() -> None:
    # Ensure ID is deterministic for same inputs
    df1 = pl.DataFrame(
        {
            "approval_id": ["A"],
            "approval_date": ["Reiwa 2.1.1"],
            "brand_name_jp": ["B"],
            "generic_name_jp": ["G"],
        }
    )
    res1 = transform_approvals_gold(df1)
    res2 = transform_approvals_gold(df1)

    assert res1["coreason_id"][0] == res2["coreason_id"][0]


def test_jader_duplicates() -> None:
    """Verify behavior when duplicate source rows exist."""
    demo = pl.DataFrame({"id": ["1"], "sex": ["M"], "age": ["20"], "reporting_year": [2020]})
    drug = pl.DataFrame({"id": ["1", "1"], "drug_name": ["D1", "D1"], "characterization": ["Suspected", "Suspected"]})
    reac = pl.DataFrame({"id": ["1"], "reaction": ["R1"]})

    result = transform_jader_gold(demo, drug, reac)
    assert len(result) == 2
    assert result.row(0) == result.row(1)


def test_date_normalization_numeric_gannen() -> None:
    """Verify 'Reiwa 1' is treated as Gannen (Year 1)."""
    assert convert_japanese_date_to_iso("Reiwa 1.5.1") == "2019-05-01"
    assert convert_japanese_date_to_iso("Reiwa 2.5.1") == "2020-05-01"


def test_jader_join_key_whitespace() -> None:
    """Verify join failure on mismatched whitespace in IDs."""
    demo = pl.DataFrame({"id": ["1 "]})  # Trailing space
    drug = pl.DataFrame({"id": ["1"], "drug_name": ["D1"], "characterization": ["Suspected"]})
    reac = pl.DataFrame({"id": ["1"], "reaction": ["R1"]})

    result = transform_jader_gold(demo, drug, reac)
    assert len(result) == 0


def test_jader_complex_cartesian_explosion() -> None:
    """
    Stress test for Cartesian logic.
    Case A: 1 Suspected Drug, 1 Reaction -> 1 Row
    Case B: 3 Suspected Drugs, 2 Reactions -> 6 Rows
    Case C: 2 Concomitant Drugs (0 Suspected), 1 Reaction -> 0 Rows
    Case D: 1 Suspected Drug, 0 Reactions -> 0 Rows
    Total Expected: 7 Rows
    """
    demo = pl.DataFrame(
        {
            "id": ["A", "B", "C", "D"],
            "sex": ["M", "F", "M", "F"],
            "age": ["10", "20", "30", "40"],
            "reporting_year": [2021, 2021, 2021, 2021],
        }
    )

    drug = pl.DataFrame(
        {
            "id": [
                "A",  # Case A (1)
                "B",
                "B",
                "B",  # Case B (3)
                "C",
                "C",  # Case C (2 Concomitant)
                "D",  # Case D (1)
            ],
            "drug_name": ["D_A", "D_B1", "D_B2", "D_B3", "D_C1", "D_C2", "D_D"],
            "characterization": [
                "Suspected",  # A
                "Suspected",
                "Suspected",
                "Suspected",  # B
                "Concomitant",
                "Concomitant",  # C
                "Suspected",  # D
            ],
        }
    )

    reac = pl.DataFrame(
        {
            "id": [
                "A",  # Case A (1)
                "B",
                "B",  # Case B (2)
                "C",  # Case C (1)
                # Case D (0)
            ],
            "reaction": ["R_A", "R_B1", "R_B2", "R_C"],
        }
    )

    result = transform_jader_gold(demo, drug, reac)

    assert len(result) == 7  # 1 (A) + 6 (B) + 0 (C) + 0 (D)

    # Verify Case B
    case_b = result.filter(pl.col("case_id") == "B").sort(["primary_suspect_drug", "reaction_pt"])
    assert len(case_b) == 6
    # Verify combination logic (first drug, first reaction)
    assert case_b.row(0, named=True)["primary_suspect_drug"] == "D_B1"
    assert case_b.row(0, named=True)["reaction_pt"] == "R_B1"
    # Verify last
    assert case_b.row(5, named=True)["primary_suspect_drug"] == "D_B3"
    assert case_b.row(5, named=True)["reaction_pt"] == "R_B2"


def test_jader_missing_optional_columns() -> None:
    """
    Verify robust handling when optional columns (e.g., age, drug_name) are missing from input.
    """
    # Demo missing 'age'
    demo = pl.DataFrame({"id": ["1"], "sex": ["M"], "reporting_year": [2021]})
    # Drug missing 'drug_name' (but has char)
    drug = pl.DataFrame({"id": ["1"], "characterization": ["Suspected"]})
    # Reac normal
    reac = pl.DataFrame({"id": ["1"], "reaction": ["R1"]})

    result = transform_jader_gold(demo, drug, reac)

    assert len(result) == 1
    # Check populated cols
    assert result["case_id"][0] == "1"
    assert result["patient_sex"][0] == "M"
    # Check missing cols became null
    assert result["patient_age_group"][0] is None
    assert result["primary_suspect_drug"][0] is None


def test_jader_null_ids() -> None:
    """Verify behavior with Null IDs (should not join)."""
    demo = pl.DataFrame({"id": [None, "1"]})
    drug = pl.DataFrame({"id": [None, "1"], "drug_name": ["D_Null", "D1"], "characterization": ["Suspected", "Suspected"]})
    reac = pl.DataFrame({"id": [None, "1"], "reaction": ["R_Null", "R1"]})

    result = transform_jader_gold(demo, drug, reac)

    # Should only match "1".
    # Polars default inner join on nulls is usually empty for nulls.
    assert len(result) == 1
    assert result["case_id"][0] == "1"


def test_approvals_date_parsing_robustness() -> None:
    """Test short era formats."""
    assert convert_japanese_date_to_iso("R2.1.1") == "2020-01-01"
    assert convert_japanese_date_to_iso("H30.1.1") == "2018-01-01"
    assert convert_japanese_date_to_iso("S64.1.7") == "1989-01-07"
    # Invalid short era
    assert convert_japanese_date_to_iso("X2.1.1") is None
