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


def test_jader_orphan_suspected_drug_preservation() -> None:
    """
    Verifies that "Suspected" drugs are preserved even if the corresponding
    Demographic (Demo) record is missing (Orphan Drug Record).

    Requirement: "Zero loss of 'Suspicion' flags due to join errors".
    """
    # Demo only has Case 1
    demo = pl.DataFrame({"id": ["1"], "sex": ["M"], "age": ["20s"], "reporting_year": [2020]})

    # Drug has Case 1 (Suspected) AND Case 2 (Suspected, but no Demo)
    drug = pl.DataFrame(
        {
            "id": ["1", "2"],
            "drug_name": ["Drug A", "Drug B"],
            "characterization": ["Suspected", "Suspected"],
        }
    )

    # Reac has Case 1 and Case 2
    reac = pl.DataFrame({"id": ["1", "2"], "reaction": ["Headache", "Nausea"]})

    # Currently, this is expected to drop Case 2 because of Inner Join with Demo.
    result = transform_jader_gold(demo, drug, reac)

    # We expect BOTH Case 1 and Case 2 to be present.
    ids = result["case_id"].to_list()

    assert "1" in ids, "Case 1 should be present"
    assert "2" in ids, "Case 2 (Orphan Suspected Drug) MUST be preserved per 'Zero Loss' requirement"

    # Verify content of Orphan Case
    row2 = result.filter(pl.col("case_id") == "2").row(0, named=True)
    assert row2["primary_suspect_drug"] == "Drug B"
    assert row2["patient_sex"] is None  # Should be null as Demo is missing
