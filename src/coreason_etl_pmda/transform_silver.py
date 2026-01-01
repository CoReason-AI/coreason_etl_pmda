# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import hashlib
import os
from typing import Any

import polars as pl
import requests

from coreason_etl_pmda.silver.schemas import SilverApprovalSchema
from coreason_etl_pmda.utils_date import convert_japanese_date_to_iso
from coreason_etl_pmda.utils_text import normalize_text

# Mapping from Japanese headers to Internal Schema
COLUMN_MAPPING = {
    "承認番号": "approval_id",
    "承認年月日": "approval_date",
    "販売名": "brand_name_jp",
    "一般的名称": "generic_name_jp",
    "申請者氏名": "applicant_name_jp",
    "薬効分類名": "indication",
}


def normalize_approvals(df: pl.DataFrame) -> pl.DataFrame:
    """
    Silver Layer Normalization for Approvals.

    1. Rename columns (Japanese -> English).
    2. Normalize text (NFKC, decoding).
    3. Normalize dates (Gannen -> ISO 8601).
    4. Generate coreason_id (Hash).
    """
    # 1. Rename columns
    # We rename columns that exist in the mapping.
    # We iterate mapping to see what we can rename.
    rename_map = {}
    for jp_col, en_col in COLUMN_MAPPING.items():
        if jp_col in df.columns:
            rename_map[jp_col] = en_col

    df = df.rename(rename_map)

    # Ensure required columns exist (create as null if missing, or error?)
    # "approval_id", "approval_date", "brand_name_jp", "generic_name_jp" are critical.
    # We'll initialize them if missing to allow partial data processing,
    # but strictly speaking Silver should enforce schema.
    # Let's ensure they exist.

    expected_cols = list(COLUMN_MAPPING.values())
    # application_type is expected from Bronze but not in mapping (it's already English/Code)
    if "application_type" not in df.columns:
        # If missing from source, default or null?
        # Bronze update ensures it's there. If older data, we might need default.
        # Let's default to "New Drug" if missing, or null?
        # Safe to default to None.
        expected_cols.append("application_type")

    for col in expected_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.String).alias(col))

    # 2. Normalize Text
    # Columns to normalize: brand_name_jp, generic_name_jp, applicant_name_jp, indication, and approval_id
    # We also normalize approval_id (trim, NFKC) to ensure ID consistency.
    text_cols = ["brand_name_jp", "generic_name_jp", "applicant_name_jp", "indication", "approval_id"]

    def norm_str(s: str | None) -> str | None:
        return normalize_text(s) if s else None

    # Apply normalization
    # We use map_elements because normalize_text handles encoding/NFKC complexity in python
    for col in text_cols:
        if col in df.columns:
            # We must handle casting to string if it's not
            # If it's Object or Utf8, fine.
            df = df.with_columns(pl.col(col).map_elements(norm_str, return_dtype=pl.String).alias(col))

    # 3. Normalize Date
    # approval_date
    def norm_date(s: str | None) -> str | None:
        return convert_japanese_date_to_iso(s) if s else None

    if "approval_date" in df.columns:
        df = df.with_columns(
            pl.col("approval_date").map_elements(norm_date, return_dtype=pl.String).alias("approval_date")
        )

    # 4. Generate coreason_id
    # Logic: Hash("PMDA" + source_id + approval_date)
    # source_id is approval_id.

    def generate_id(struct: dict[str, Any]) -> str:
        sid = struct.get("approval_id")
        date = struct.get("approval_date")
        # Handle None
        sid_str = str(sid) if sid is not None else ""
        date_str = str(date) if date is not None else ""

        raw = f"PMDA{sid_str}{date_str}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    df = df.with_columns(
        pl.struct(["approval_id", "approval_date"])
        .map_elements(generate_id, return_dtype=pl.String)
        .alias("coreason_id")
    )

    # 5. Pydantic Validation
    # We validate the output schema using Pydantic.
    # We can iterate and validate or use polars schema validation if we strictly typed it,
    # but requirement is "polars + pydantic".
    # For performance on large data, doing this row-by-row in python is slow.
    # But for "100% correctness" and "pydantic" requirement, we do it.
    # We can skip if data is massive, but here we comply with the protocol.

    def validate_row(row: dict[str, Any]) -> dict[str, Any]:
        # Filter keys to match schema
        return SilverApprovalSchema(**row).model_dump()

    # Apply validation (this ensures types and extra fields are ignored/handled)
    # Note: map_elements on struct returns struct.
    # We might need to redefine schema.
    # A simpler way is to just let Pydantic check validity and pass through, or clean data.
    # Let's clean.

    # To do this efficiently in Polars:
    # Converting to dicts and back is expensive.
    # We will assume if it passes the logic above it's mostly fine,
    # but we will run a check.

    # Implementation:
    # Convert to python objects, validate, create new DF.
    # This is the most robust "Pydantic" way.

    rows = df.to_dicts()
    validated_rows = [validate_row(r) for r in rows]

    # Re-create DataFrame from validated rows to ensure schema compliance
    return pl.DataFrame(validated_rows)


def jan_bridge_lookup(approvals_df: pl.DataFrame, jan_df: pl.DataFrame) -> pl.DataFrame:
    """
    Step 1: Deterministic Lookup.
    Left Join pmda_approvals with bronze_ref_jan_inn on generic_name_jp == jan_name_jp.
    Populates generic_name_en.
    """
    if "generic_name_jp" not in approvals_df.columns:
        raise ValueError("approvals_df must have generic_name_jp")
    if "jan_name_jp" not in jan_df.columns:
        raise ValueError("jan_df must have jan_name_jp")

    # Join
    joined = approvals_df.join(jan_df, left_on="generic_name_jp", right_on="jan_name_jp", how="left")

    # Coalesce: inn_name_en -> jan_name_en -> generic_name_en (target)
    # We prefer INN if available.
    joined = joined.with_columns(pl.coalesce(["inn_name_en", "jan_name_en"]).alias("generic_name_en"))

    return joined


def jan_bridge_ai_fallback(df: pl.DataFrame) -> pl.DataFrame:
    """
    Step 2: Reasoning Fallback.
    Iterates rows where generic_name_en is NULL.
    Invokes Mock DeepSeek API.
    """
    # Identify missing translations
    missing_mask = df["generic_name_en"].is_null()

    if not missing_mask.any():
        return df

    def translate(struct: dict[str, Any]) -> str | None:
        generic_jp = struct.get("generic_name_jp")
        brand_jp = struct.get("brand_name_jp")

        if not generic_jp:
            return None

        return call_deepseek(generic_jp, str(brand_jp) if brand_jp else "")

    rows = df.to_dicts()
    updated_rows = []

    for row in rows:
        if row.get("generic_name_en") is None:
            # Translation needed
            trans = translate(row)
            row["generic_name_en"] = trans
            if trans is None:
                row["_translation_status"] = "failed"
            else:
                row["_translation_status"] = "ai_translated"
        else:
            row["_translation_status"] = "lookup_success"
        updated_rows.append(row)

    return pl.DataFrame(updated_rows, schema_overrides=df.schema)


def call_deepseek(generic_name_jp: str, brand_name_jp: str) -> str | None:
    """
    Calls the DeepSeek API (or compatible) to translate the Japanese generic name.
    """
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        # If no key, we can't call.
        return None

    base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/chat/completions")

    prompt = (
        f"Translate the Japanese pharmaceutical ingredient '{generic_name_jp}' to its English INN. "
        f"Context: Brand is '{brand_name_jp}'. Return ONLY the English name."
    )

    payload = {
        "model": "deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
    }

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        response = requests.post(base_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        # Parse: data["choices"][0]["message"]["content"]
        content = data["choices"][0]["message"]["content"]
        return content.strip() if content else None
    except Exception:
        # Log error?
        return None
