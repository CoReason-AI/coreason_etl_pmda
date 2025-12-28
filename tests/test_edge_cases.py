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
    # multiple separators might parse if regex finds numbers
    assert convert_japanese_date_to_iso("Reiwa 2..5..1") == "2020-05-01"
    # My regex is `re.findall(r"\d+", remaining)`. So "2..5..1" -> ["2", "5", "1"] -> month 2, day 5 ??
    # Wait, remaining starts AFTER year.
    # "Reiwa 2..5..1". "Reiwa" matches era. "2" matches year. Remaining: "..5..1".
    # re.findall(\d+) -> ["5", "1"]. Month 5, Day 1. Correct.
    assert convert_japanese_date_to_iso("Reiwa 2//5//1") == "2020-05-01"

    # Full width numbers in date string
    # "Reiwa ２.５.１"
    # My regex uses `\d+`. Python `\d` matches full width digits.
    assert convert_japanese_date_to_iso("Reiwa ２.５.１") == "2020-05-01"

    # Mixed eras? No, regex picks first valid one.


def test_text_edge_cases() -> None:
    # Text with mixed valid/invalid utf-8 sequences?
    # normalize_text tries encodings.
    # Case: Empty string
    assert normalize_text("") == ""

    # Case: Byte sequence that is valid in CP932 but not UTF-8
    # "日本語" in CP932 is b'\x93\xfa\x96{\x8c\xea'
    cp932_bytes = "日本語".encode("cp932")
    assert normalize_text(cp932_bytes) == "日本語"

    # Case: Byte sequence that decodes in both but differently?
    # Rare, but `utf-8` is tried first.
    # If we pass bytes that are valid cp932 but look like invalid utf-8, it falls back.


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
    # It is case sensitive.
    demo = pl.DataFrame({"id": ["1"]})
    drug = pl.DataFrame({"id": ["1"], "drug_name": ["D1"], "characterization": ["suspected"]})
    reac = pl.DataFrame({"id": ["1"], "reaction": ["R1"]})

    result = transform_jader_gold(demo, drug, reac)
    assert len(result) == 0  # Should fail if strict "Suspected"


def test_approvals_gold_id_stability() -> None:
    # Ensure ID is deterministic for same inputs
    # Use Japanese dates because transform normalizes them.
    # ISO inputs might fail the era regex and result in None date.
    df1 = pl.DataFrame(
        {
            "approval_id": ["A"],
            "approval_date": ["Reiwa 2.1.1"],
            "brand_name_jp": ["B"],
            "generic_name_jp": ["G"],
        }
    )
    df2 = pl.DataFrame(
        {
            "approval_id": ["A"],
            "approval_date": ["Reiwa 2.1.1"],
            "brand_name_jp": ["B"],
            "generic_name_jp": ["G"],
        }
    )

    res1 = transform_approvals_gold(df1)
    res2 = transform_approvals_gold(df2)

    assert res1["coreason_id"][0] == res2["coreason_id"][0]

    # Ensure ID changes if date changes
    df3 = pl.DataFrame(
        {
            "approval_id": ["A"],
            "approval_date": ["Reiwa 2.1.2"],
            "brand_name_jp": ["B"],
            "generic_name_jp": ["G"],
        }
    )
    res3 = transform_approvals_gold(df3)
    assert res1["coreason_id"][0] != res3["coreason_id"][0]
