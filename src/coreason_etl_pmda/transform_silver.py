# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda


from typing import Any

import polars as pl


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
    # jan_df columns: jan_name_jp, jan_name_en, inn_name_en
    # We want generic_name_en from jan_name_en or inn_name_en?
    # Spec says: "Output: Populate generic_name_en from the reference table."
    # The reference table has `jan_name_en` and `inn_name_en`.
    # Usually INN is preferred? Spec says "Map Japanese drugs to their English INN".
    # So we prefer `inn_name_en`? Or `jan_name_en`?
    # Spec says target is `generic_name_en`.
    # Let's assume `jan_name_en` is the English JAN, which is usually the INN.
    # Let's take `jan_name_en` as per Spec "bronze_ref_jan_inn (jan_name_jp, jan_name_en, inn_name_en)".
    # Wait, spec says "Output: Populate generic_name_en from the reference table."
    # Later "Map Japanese drugs to their English INN".
    # The reference table schema provided in spec is "bronze_ref_jan_inn (jan_name_jp, jan_name_en, inn_name_en)".
    # I will use `inn_name_en` if available, else `jan_name_en`.

    # We'll rename `inn_name_en` (or `jan_name_en`) to `generic_name_en` after join.

    joined = approvals_df.join(jan_df, left_on="generic_name_jp", right_on="jan_name_jp", how="left")

    # Coalesce: inn_name_en -> jan_name_en -> generic_name_en (target)
    # The spec says "Output: Populate generic_name_en".

    # We need to construct the column `generic_name_en`.
    # If `inn_name_en` exists, use it. Else `jan_name_en`.

    # Polars coalesce
    joined = joined.with_columns(pl.coalesce(["inn_name_en", "jan_name_en"]).alias("generic_name_en"))

    # Drop temp columns
    # We keep standard columns.

    return joined


def jan_bridge_ai_fallback(df: pl.DataFrame) -> pl.DataFrame:
    """
    Step 2: Reasoning Fallback.
    Iterates rows where generic_name_en is NULL.
    Invokes Mock DeepSeek API.
    """
    # Identify missing translations
    # If generic_name_en is null

    # We cannot easily iterate and update polars DF row by row efficiently for API calls without map_elements
    # or separate list. We'll extract rows needing translation.

    # Filter for missing
    missing_mask = df["generic_name_en"].is_null()

    if not missing_mask.any():
        return df

    # Apply translation
    # We define a function to call the API

    def translate(struct: dict[str, Any]) -> str | None:
        generic_jp = struct.get("generic_name_jp")
        brand_jp = struct.get("brand_name_jp")

        if not generic_jp:
            return None

        # Mock DeepSeek API
        # "Prompt: Translate ... Context: Brand ..."
        # For this implementation, we Mock it.
        # But we need to allow testing to mock this internal call.
        # We can put the API call in a separate function `call_deepseek` and mock that.

        return call_deepseek(generic_jp, str(brand_jp) if brand_jp else "")

    # We use map_elements (apply)
    # Note: map_elements in Polars 1.0+?
    # We pass a struct of necessary columns.

    # We only update the nulls.
    # We can create a new series for the missing ones.

    # However, Polars `map_elements` is slow (python loop).
    # Since this is an AI call, latency is dominated by network, so python loop is fine.

    # We need to apply on the filtered rows and then update the original DF.

    # Create a small DF of missing
    # missing_df = df.filter(missing_mask)

    # Apply translation
    # We iterate rows manually below, so this select is unused but shows intent.
    # We can remove it to satisfy linter.
    # translations = missing_df.select(
    #     pl.struct(["generic_name_jp", "brand_name_jp"])
    #     .map_elements(translate, return_dtype=pl.String)
    #     .alias("generic_name_en_ai")
    # )

    # We need to merge this back.
    # A robust way is to join on index or PK?
    # Or just update the column conditionally?
    # Polars update:
    # df = df.with_columns(
    #    pl.when(pl.col("generic_name_en").is_null())
    #    .then(pl.struct(...).map_elements(...))
    #    .otherwise(pl.col("generic_name_en"))
    # )

    # This runs map_elements only on the needed rows?
    # Polars executes expressions eagerly or lazy. map_elements is opaque.
    # It might run on all if not careful.
    # But inside `when().then()`, it should apply only to true?
    # Actually `map_elements` on a Series/Expr applies to the whole Series usually.
    # So we should be careful.

    # Better to process only missing, then stack/update?
    # But `update` requires join key or index alignment.

    # Let's try the `when-then` approach but optimized:
    # We can't optimize map_elements easily inside expression if we want to avoid calling it for existing ones.
    # A python-side iteration might be safer for "Separate Step".

    # Strategy:
    # 1. Convert DF to dicts/rows.
    # 2. Iterate and update.
    # 3. Re-create DF.
    # This is safe and simple for this "Atomic Unit" context.

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


def call_deepseek(generic_name_jp: str, brand_name_jp: str) -> str | None:  # pragma: no cover
    """
    Mock DeepSeek API call.
    In real life, this calls the API.
    Here we return None (fail) or mock via test patching.
    """
    # Return None by default (Miss)
    return None
