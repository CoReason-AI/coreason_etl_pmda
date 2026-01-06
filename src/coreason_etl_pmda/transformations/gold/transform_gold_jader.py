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


def transform_jader_gold(demo_df: pl.DataFrame, drug_df: pl.DataFrame, reac_df: pl.DataFrame) -> pl.DataFrame:
    """
    Reconstructs JADER data into Gold `pmda_adverse_events` table.

    Logic:
    1. Anchor: drug.csv (Filtered for "Suspected")
    2. Join: demo.csv (Left Join) - Preserves orphan suspected drugs.
    3. Join: reac.csv (Left Join)

    Produces a Cartesian product of Suspected Drugs x Reactions per Case.

    Schema:
    - case_id (from drug.id)
    - patient_sex (from demo.sex)
    - patient_age_group (from demo.age)
    - primary_suspect_drug (from drug.drug_name)
    - reaction_pt (from reac.reaction)
    - reporting_year (from demo.reporting_year)
    """

    # 0. Validation
    # Ensure critical join keys exist
    join_key = "id"
    if join_key not in demo_df.columns:
        raise ValueError(f"demo_df missing key: {join_key}")
    if join_key not in drug_df.columns:
        raise ValueError(f"drug_df missing key: {join_key}")
    if join_key not in reac_df.columns:
        raise ValueError(f"reac_df missing key: {join_key}")

    # Ensure drug_df has characterization for filtering
    if "characterization" not in drug_df.columns:
        raise ValueError("drug_df missing 'characterization' column for filtering suspected drugs")

    # 1. Filter Drug for "Suspected" and Valid ID
    # We strictly filter for "Suspected".
    # Note: Silver layer normalizes "被疑薬" -> "Suspected".
    # We also discard rows where 'id' is Null, as a case must have an ID.
    suspect_drugs = drug_df.filter((pl.col("characterization") == "Suspected") & (pl.col(join_key).is_not_null()))

    # 2. Join Drug + Demo (Left Join)
    # This anchors on Suspected Drugs and pulls in Demo info if available.
    # Prevents loss of Suspected Drugs if Demo record is missing.
    # Note: demo_df columns might conflict with drug_df columns.
    # Usually 'id' collides.
    base_drug = suspect_drugs.join(demo_df, on=join_key, how="left", suffix="_demo")

    # 3. Join Reac (Left Join)
    # This creates the Cartesian product: (Suspected Drugs) x (Reactions)
    # We use LEFT JOIN to ensure cases with Suspected Drugs are preserved even if Reaction data is missing.
    final = base_drug.join(reac_df, on=join_key, how="left", suffix="_reac")

    # 4. Select and Rename Columns
    # We define the target schema mapping.
    # Source Column -> Target Column
    # If source column is missing (optional in Silver), we fill with Null.

    # Helper to safe select
    def safe_col(name: str, alias: str) -> pl.Expr:
        if name in final.columns:
            return pl.col(name).alias(alias)
        return pl.lit(None).alias(alias)

    # Identify source column names.
    # From Silver JADER:
    # Demo: id, sex, age, reporting_year
    # Drug: id, drug_name, characterization
    # Reac: id, reaction

    # Note on 'id': 'id' from drug_df (suspect_drugs) is preserved as 'id'.
    # If demo_df had 'id', it was joined on it.

    # We take 'id' as case_id.

    target_cols = [
        safe_col("id", "case_id"),
        safe_col("sex", "patient_sex"),
        safe_col("age", "patient_age_group"),
        safe_col("drug_name", "primary_suspect_drug"),
        safe_col("reaction", "reaction_pt"),
        safe_col("reporting_year", "reporting_year"),
    ]

    return final.select(target_cols)
