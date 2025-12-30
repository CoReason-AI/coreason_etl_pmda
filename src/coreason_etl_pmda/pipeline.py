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
from typing import Any

import dlt

from coreason_etl_pmda.sources_approvals import approvals_source
from coreason_etl_pmda.sources_jader import jader_source
from coreason_etl_pmda.sources_jan import jan_inn_source
from coreason_etl_pmda.utils_logger import logger

# Note: Silver/Gold transformations are usually done via dlt transformer or DBT.
# Since we used Polars for transformations in the spec (mandatory for JADER CSV velocity),
# we might run them as post-load steps or as dlt transformers if we stream data.
# The spec said: "Transformation: polars".
# "Orchestration: Stateless; triggered via external orchestrator."
#
# So this pipeline file mainly defines the Ingestion (Bronze).
# Transformations would be separate steps or invoked here after load.
# Given dlt loads to destination (DuckDB), we can fetch from DuckDB -> Polars -> DuckDB for Silver/Gold?
# Or if the data volume allows, we can transform in-flight?
# JADER Zip -> Extract CSV -> Polars (Transform) -> Load?
# The spec for JADER Ingestion says: "Download Zip -> Extract CSVs -> Load to Bronze".
# Then Silver/Gold are layers.
#
# So we will define a pipeline that loads Bronze.
# And functions to run Silver/Gold using the loaded data (if we use dlt to load back) or just separate scripts.
# I will define the Bronze Ingestion Pipeline here.


@logger.catch  # type: ignore[misc]
def run_bronze_pipeline(
    destination: Any = "duckdb",
    dataset_name: str = "pmda_bronze",
    duckdb_path: str | None = None,
) -> dlt.Pipeline:
    """
    Runs the Bronze Layer Ingestion.
    """
    # If duckdb_path is provided, we configure the destination to use it.
    # dlt accepts a connection string like "duckdb:///path/to.db"
    # or we can pass a dlt destination object.
    if duckdb_path:
        # Check if destination is the string "duckdb"
        if destination == "duckdb":
            destination = f"duckdb:///{duckdb_path}"
        # If user passed a custom destination object, we ignore duckdb_path or warn?
        # We assume if duckdb_path is set, they want to use it.

    # Fallback to env var if not explicitly passed but configured in environment
    if not duckdb_path and destination == "duckdb":
        env_path = os.getenv("DUCKDB_PATH")
        if env_path:
            destination = f"duckdb:///{env_path}"

    p = dlt.pipeline(
        pipeline_name="coreason_etl_pmda_bronze",
        destination=destination,
        dataset_name=dataset_name,
    )

    # Run sources
    # We use list to ensure all run
    sources = [jan_inn_source(), approvals_source(), jader_source()]

    info = p.run(sources)
    logger.info(info)
    return info


if __name__ == "__main__":
    run_bronze_pipeline()  # pragma: no cover
