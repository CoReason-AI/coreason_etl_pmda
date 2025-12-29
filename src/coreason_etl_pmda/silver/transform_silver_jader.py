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

from coreason_etl_pmda.utils_text import normalize_text

# Mappings (Japanese -> English)
DEMO_MAPPING = {
    "識別番号": "id",
    "性別": "sex",
    "年齢": "age",
    "報告年度": "reporting_year",
}

DRUG_MAPPING = {
    "識別番号": "id",
    "医薬品（一般名）": "drug_name",
    "被疑薬等区分": "characterization",
}

REAC_MAPPING = {
    "識別番号": "id",
    "有害事象": "reaction",
}

# Value Mappings
CHARACTERIZATION_MAPPING = {
    "被疑薬": "Suspected",
    "併用薬": "Concomitant",
}


import re

def _normalize_common(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    """
    Common normalization logic:
    1. Rename columns based on mapping.
    2. Normalize text (NFKC) for all string columns.
    """
    # Normalize headers first (strip whitespace)
    # This ensures "識別 番号" maps to "識別番号" if we clean it first.
    # However, we can't just strip keys in mapping, we must strip DF columns.

    # We rename columns in DF to be whitespace-free if they match mapping keys sans whitespace.
    # Or simpler: normalize all DF columns by removing whitespace.

    new_cols = {col: re.sub(r"\s+", "", col) for col in df.columns}
    df = df.rename(new_cols)

    # Rename based on mapping
    rename_map = {}
    for jp_col, en_col in mapping.items():
        if jp_col in df.columns:
            rename_map[jp_col] = en_col
        # We also support if the column is already in English (idempotent)
        # but the source is expected to be Japanese.

    df = df.rename(rename_map)

    # Validate schema presence
    # We strictly require keys from mapping to be present (as English now)
    missing = [en for _, en in mapping.items() if en not in df.columns]
    if missing:
        # For robustness, we might allow missing columns if they are not PK?
        # But JADER is structured. 'id' is critical.
        if "id" in missing:
            raise ValueError(f"Missing critical column 'id' (mapped from '識別番号'). Columns: {df.columns}")
        # For others, we might warn or create nulls?
        # Let's create nulls for now to match other patterns in this repo.
        for col in missing:
            df = df.with_columns(pl.lit(None).alias(col))

    # Normalize Text
    # We apply normalize_text to string columns.
    # Note: 'id' might be int or string. We should cast 'id' to string for consistency?
    # Gold layer expects 'id' to join.
    # PMDA IDs are strings/ints.
    # Let's cast to String and normalize.

    # Identify string columns
    # We explicitly normalize known text columns.
    # Demo: sex, age
    # Drug: drug_name, characterization
    # Reac: reaction

    cols_to_normalize = []
    for col in df.columns:
        if col in ["sex", "age", "drug_name", "characterization", "reaction", "id"]:
            cols_to_normalize.append(col)

    def norm_str(s: str | None) -> str | None:
        return normalize_text(s)

    for col in cols_to_normalize:
        # Cast to String first
        df = df.with_columns(pl.col(col).cast(pl.String).map_elements(norm_str, return_dtype=pl.String).alias(col))

    return df


def normalize_jader_demo(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalizes JADER Demo table.
    """
    df = _normalize_common(df, DEMO_MAPPING)
    return df


def normalize_jader_drug(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalizes JADER Drug table.
    Maps 'characterization' values (e.g. 被疑薬 -> Suspected).
    """
    df = _normalize_common(df, DRUG_MAPPING)

    # Value Mapping for characterization
    if "characterization" in df.columns:
        # replace values
        # We can use replace? or join?
        # Polars replace is `replace(old, new)` or `replace_strict`.
        # `replace` accepts a mapping.
        # However, `replace` in polars might be per Series.

        # We want to map known values and leave others or null?
        # Spec says "Zero loss of 'Suspicion' flags".
        # If text is normalized, it should match keys in CHARACTERIZATION_MAPPING.

        # Logic:
        # 1. Map known values.
        # 2. Unknown values? Maybe leave as is (Japanese) so we don't lose info?
        # Gold layer filters `characterization == "Suspected"`.
        # So as long as "Suspected" is mapped correctly, we are good.

        # We can use `pl.col("characterization").replace(CHARACTERIZATION_MAPPING)`
        # `replace` uses `default` for unmatched. We set default to original value.

        df = df.with_columns(
            pl.col("characterization")
            .replace_strict(CHARACTERIZATION_MAPPING, default=pl.col("characterization"))
            .alias("characterization")
        )

    return df


def normalize_jader_reac(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalizes JADER Reaction table.
    """
    df = _normalize_common(df, REAC_MAPPING)
    return df
