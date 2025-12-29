# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import os

import duckdb
import polars as pl
from loguru import logger

from coreason_etl_pmda.pipeline import run_bronze_pipeline
from coreason_etl_pmda.silver.transform_silver_jader import (
    normalize_jader_demo,
    normalize_jader_drug,
    normalize_jader_reac,
)
from coreason_etl_pmda.transform_gold_approvals import transform_approvals_gold
from coreason_etl_pmda.transform_gold_jader import transform_jader_gold
from coreason_etl_pmda.transform_silver import (
    jan_bridge_ai_fallback,
    jan_bridge_lookup,
    normalize_approvals,
)


def get_duckdb_path() -> str:
    return os.getenv("DUCKDB_PATH", "pmda.duckdb")


def write_to_duckdb(con: duckdb.DuckDBPyConnection, df: pl.DataFrame, table_name: str) -> None:
    """
    Writes a Polars DataFrame to DuckDB, replacing the table if it exists.
    """
    if df.is_empty() and len(df.columns) == 0:
        logger.warning(f"Skipping write of empty dataframe (0 cols) to {table_name}")
        return

    logger.info(f"Writing {table_name} to DuckDB ({len(df)} rows)...")
    con.register("temp_view_for_write", df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_view_for_write")
    con.unregister("temp_view_for_write")


def process_silver_approvals(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("Processing Silver Approvals...")
    tables = [x[0] for x in con.execute("SHOW TABLES").fetchall()]

    if "bronze_approvals" not in tables:
        logger.warning("Table 'bronze_approvals' not found. Skipping Approvals Silver.")
        silver_approvals = pl.DataFrame()
    else:
        bronze_approvals = pl.read_database("SELECT * FROM bronze_approvals", con)

        # Normalize
        silver_approvals = normalize_approvals(bronze_approvals)

        # JAN Bridge
        if "bronze_ref_jan_inn" in tables:
            bronze_jan = pl.read_database("SELECT * FROM bronze_ref_jan_inn", con)
            silver_approvals = jan_bridge_lookup(silver_approvals, bronze_jan)
        else:
            logger.warning("Table 'bronze_ref_jan_inn' not found. Skipping JAN Lookup.")
            if "generic_name_en" not in silver_approvals.columns:
                silver_approvals = silver_approvals.with_columns(pl.lit(None).cast(pl.String).alias("generic_name_en"))

        # AI Fallback
        api_key = os.getenv("DEEPSEEK_API_KEY")
        if api_key:
            logger.info("DeepSeek API Key found. Running AI Fallback...")
            silver_approvals = jan_bridge_ai_fallback(silver_approvals)
        else:  # pragma: no cover
            logger.warning("DEEPSEEK_API_KEY not found. Skipping AI Fallback.")
            # Set status to skipped (overwriting if exists, or creating new)
            silver_approvals = silver_approvals.with_columns(pl.lit("skipped_no_key").alias("_translation_status"))

    write_to_duckdb(con, silver_approvals, "silver_approvals")


def process_silver_jader(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("Processing Silver JADER...")
    tables = [x[0] for x in con.execute("SHOW TABLES").fetchall()]
    jader_tables = ["bronze_jader_demo", "bronze_jader_drug", "bronze_jader_reac"]
    missing_jader = [t for t in jader_tables if t not in tables]

    if missing_jader:
        logger.warning(f"Missing JADER tables: {missing_jader}. Skipping JADER Silver.")
    else:
        demo = pl.read_database("SELECT * FROM bronze_jader_demo", con)
        drug = pl.read_database("SELECT * FROM bronze_jader_drug", con)
        reac = pl.read_database("SELECT * FROM bronze_jader_reac", con)

        silver_demo = normalize_jader_demo(demo)
        silver_drug = normalize_jader_drug(drug)
        silver_reac = normalize_jader_reac(reac)

        write_to_duckdb(con, silver_demo, "silver_jader_demo")
        write_to_duckdb(con, silver_drug, "silver_jader_drug")
        write_to_duckdb(con, silver_reac, "silver_jader_reac")


def run_silver_transformation(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("Starting Silver Layer Transformations...")

    try:
        process_silver_approvals(con)
    except Exception as e:  # pragma: no cover
        logger.error(f"Error in Silver Approvals: {e}")  # pragma: no cover
        raise  # pragma: no cover

    try:
        process_silver_jader(con)
    except Exception as e:  # pragma: no cover
        logger.error(f"Error in Silver JADER: {e}")  # pragma: no cover
        raise  # pragma: no cover


def run_gold_transformation(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("Starting Gold Layer Transformations...")

    tables = [x[0] for x in con.execute("SHOW TABLES").fetchall()]

    # --- Gold Approvals ---
    if "silver_approvals" in tables:
        silver_approvals = pl.read_database("SELECT * FROM silver_approvals", con)
        # Gold Transformation
        # We assume silver_approvals matches schema expected by gold (it should if Silver ran)
        gold_approvals = transform_approvals_gold(silver_approvals)
        write_to_duckdb(con, gold_approvals, "gold_pmda_approvals")
    else:
        logger.warning("silver_approvals not found. Skipping Gold Approvals.")

    # --- Gold JADER ---
    if all(t in tables for t in ["silver_jader_demo", "silver_jader_drug", "silver_jader_reac"]):
        demo = pl.read_database("SELECT * FROM silver_jader_demo", con)
        drug = pl.read_database("SELECT * FROM silver_jader_drug", con)
        reac = pl.read_database("SELECT * FROM silver_jader_reac", con)

        gold_jader = transform_jader_gold(demo, drug, reac)
        write_to_duckdb(con, gold_jader, "gold_pmda_adverse_events")
    else:
        logger.warning("Silver JADER tables missing. Skipping Gold JADER.")


def run_full_pipeline() -> None:
    """
    Orchestrates the full Bronze -> Silver -> Gold pipeline.
    """
    db_path = get_duckdb_path()
    logger.info(f"Target DuckDB: {db_path}")

    abs_db_path = os.path.abspath(db_path)
    conn_str = f"duckdb:///{abs_db_path}"
    os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = conn_str

    logger.info("--- Step 1: Bronze Layer (Ingestion) ---")
    run_bronze_pipeline()

    con = duckdb.connect(db_path)
    try:
        logger.info("--- Step 2: Silver Layer (Refinery) ---")
        run_silver_transformation(con)
        logger.info("--- Step 3: Gold Layer (Product) ---")
        run_gold_transformation(con)
    finally:
        con.close()
        logger.info("Pipeline Complete.")


if __name__ == "__main__":  # pragma: no cover
    run_full_pipeline()
