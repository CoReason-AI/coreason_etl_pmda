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
from pathlib import Path

import duckdb
import polars as pl

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
from coreason_etl_pmda.utils_logger import logger


class PipelineOrchestrator:
    def __init__(self, duckdb_path: str = "pmda.duckdb"):
        self.duckdb_path = duckdb_path
        # Ensure path exists or let duckdb create it
        self.con = duckdb.connect(self.duckdb_path)

    def close(self) -> None:
        self.con.close()

    def run_bronze(self) -> None:
        """
        Executes the Bronze Layer (Ingestion).
        Delegates to dlt pipeline.
        """
        logger.info("Starting Bronze Layer...")
        # Close connection briefly to allow dlt to open it exclusively if needed (though duckdb supports concurrency if read/write is managed)
        # dlt usually requires exclusive lock if it writes? DuckDB 0.10+ has better concurrency but better safe.
        self.con.close()

        try:
            run_bronze_pipeline(duckdb_path=self.duckdb_path)
        finally:
            # Reopen
            self.con = duckdb.connect(self.duckdb_path)

        logger.info("Bronze Layer Complete.")

    def run_silver(self) -> None:
        """
        Executes Silver Layer (Transformation).
        Reads Bronze -> Transforms -> Writes Silver.
        """
        logger.info("Starting Silver Layer...")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS pmda_silver")

        # 1. Approvals
        self._run_silver_approvals()

        # 2. JADER
        self._run_silver_jader()

        logger.info("Silver Layer Complete.")

    def _run_silver_approvals(self) -> None:
        logger.info("Processing Silver Approvals...")

        # Read Bronze Tables
        # dlt tables are likely in `pmda_bronze` schema or main?
        # dlt dataset_name="pmda_bronze" -> schema "pmda_bronze".
        # We need to verify if dlt created a schema or just prefixed tables.
        # Usually `dataset_name` in dlt with DuckDB creates a schema `pmda_bronze`.

        bronze_schema = "pmda_bronze"

        # Check if tables exist
        try:
            approvals_df = self.con.sql(f"SELECT * FROM {bronze_schema}.bronze_approvals").pl()
            jan_df = self.con.sql(f"SELECT * FROM {bronze_schema}.bronze_ref_jan_inn").pl()
        except duckdb.Error:
            logger.warning("Bronze tables not found. Skipping Silver Approvals.")
            return

        if approvals_df.height == 0:
            logger.warning("Bronze Approvals is empty.")
            return

        # Normalize
        silver_approvals = normalize_approvals(approvals_df)

        # JAN Bridge
        if jan_df.height > 0:
            silver_approvals = jan_bridge_lookup(silver_approvals, jan_df)
        else:
            # If no JAN data, we might need to add the column as null
            logger.warning("JAN Reference data empty/missing. Skipping Lookup.")
            silver_approvals = silver_approvals.with_columns(pl.lit(None).cast(pl.String).alias("generic_name_en"))

        # AI Fallback
        # Check env var for key
        if os.getenv("DEEPSEEK_API_KEY"):
            silver_approvals = jan_bridge_ai_fallback(silver_approvals)
        else:
            logger.info("DEEPSEEK_API_KEY not set. Skipping AI Fallback.")
            # Ensure status column exists
            if "_translation_status" not in silver_approvals.columns:
                 silver_approvals = silver_approvals.with_columns(pl.lit("skipped_no_key").alias("_translation_status"))

        # Write to Silver
        # We write to `pmda_silver` schema? Or just `silver_approvals` in main?
        # Let's use `pmda_silver` schema.
        # self.con.execute("CREATE SCHEMA IF NOT EXISTS pmda_silver") # Moved to run_silver
        self._write_table("pmda_silver.silver_approvals", silver_approvals)

    def _run_silver_jader(self) -> None:
        logger.info("Processing Silver JADER...")
        bronze_schema = "pmda_bronze"

        tables = ["bronze_jader_demo", "bronze_jader_drug", "bronze_jader_reac"]
        dfs = {}

        for t in tables:
            try:
                dfs[t] = self.con.sql(f"SELECT * FROM {bronze_schema}.{t}").pl()
            except duckdb.Error:
                logger.warning(f"Table {t} not found.")
                dfs[t] = pl.DataFrame()

        # Demo
        if not dfs["bronze_jader_demo"].is_empty():
            demo_silver = normalize_jader_demo(dfs["bronze_jader_demo"])
            self._write_table("pmda_silver.silver_jader_demo", demo_silver)

        # Drug
        if not dfs["bronze_jader_drug"].is_empty():
            drug_silver = normalize_jader_drug(dfs["bronze_jader_drug"])
            self._write_table("pmda_silver.silver_jader_drug", drug_silver)

        # Reac
        if not dfs["bronze_jader_reac"].is_empty():
            reac_silver = normalize_jader_reac(dfs["bronze_jader_reac"])
            self._write_table("pmda_silver.silver_jader_reac", reac_silver)

    def run_gold(self) -> None:
        """
        Executes Gold Layer (Product Schema).
        Reads Silver -> Transforms -> Writes Gold.
        """
        logger.info("Starting Gold Layer...")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS pmda_gold")

        # 1. Approvals
        try:
            silver_approvals = self.con.sql("SELECT * FROM pmda_silver.silver_approvals").pl()
            if not silver_approvals.is_empty():
                gold_approvals = transform_approvals_gold(silver_approvals)
                self._write_table("pmda_gold.pmda_approvals", gold_approvals)
        except duckdb.Error:
            logger.warning("Silver Approvals not found. Skipping Gold Approvals.")

        # 2. JADER (Reconstruction)
        try:
            demo = self.con.sql("SELECT * FROM pmda_silver.silver_jader_demo").pl()
            drug = self.con.sql("SELECT * FROM pmda_silver.silver_jader_drug").pl()
            reac = self.con.sql("SELECT * FROM pmda_silver.silver_jader_reac").pl()

            if not demo.is_empty() and not drug.is_empty() and not reac.is_empty():
                gold_jader = transform_jader_gold(demo, drug, reac)
                self._write_table("pmda_gold.pmda_adverse_events", gold_jader)
            else:
                logger.warning("One or more Silver JADER tables empty. Skipping Gold JADER.")

        except duckdb.Error:
             logger.warning("Silver JADER tables not found. Skipping Gold JADER.")

        logger.info("Gold Layer Complete.")

    def _write_table(self, table_name: str, df: pl.DataFrame) -> None:
        """
        Writes Polars DataFrame to DuckDB table.
        """
        # Register view
        # We need a unique view name to avoid collision?
        # Or just register as 'temp_view' and drop it?
        view_name = f"view_{table_name.replace('.', '_')}"

        # DuckDB python api doesn't support direct registration of Polars DF in all versions?
        # It does if we use `.pl()` and `duckdb.sql("...").pl()`.
        # To write: `con.execute("CREATE TABLE x AS SELECT * FROM df")`.
        # But we need to pass `df` to sql context.
        # `con.register(name, df)` works for pandas. For Polars, recent DuckDB supports it.
        # If not, we convert to Arrow.

        try:
            self.con.register(view_name, df)
            self.con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {view_name}")
            self.con.unregister(view_name)
            logger.info(f"Wrote {df.height} rows to {table_name}")
        except Exception as e:
            logger.error(f"Failed to write {table_name}: {e}")
            raise

def run_full_pipeline(duckdb_path: str | None = None) -> None:
    path = duckdb_path or os.getenv("DUCKDB_PATH", "pmda.duckdb")
    orchestrator = PipelineOrchestrator(path)
    try:
        orchestrator.run_bronze()
        orchestrator.run_silver()
        orchestrator.run_gold()
    finally:
        orchestrator.close()

if __name__ == "__main__":
    run_full_pipeline()  # pragma: no cover
