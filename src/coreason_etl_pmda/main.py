# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import click

from coreason_etl_pmda.pipeline_full import PipelineOrchestrator
from coreason_etl_pmda.utils_logger import logger


@click.group()
@click.option("--duckdb-path", default="pmda.duckdb", help="Path to DuckDB database file.")
@click.pass_context
def cli(ctx: click.Context, duckdb_path: str) -> None:
    """CoReason ETL PMDA Pipeline CLI."""
    ctx.ensure_object(dict)
    ctx.obj["duckdb_path"] = duckdb_path
    logger.info(f"CLI started. Database: {duckdb_path}")


@cli.command()
@click.pass_context
def run_all(ctx: click.Context) -> None:
    """Run the full pipeline (Bronze -> Silver -> Gold)."""
    db_path = ctx.obj["duckdb_path"]
    orchestrator = PipelineOrchestrator(db_path)
    try:
        orchestrator.run_bronze()
        orchestrator.run_silver()
        orchestrator.run_gold()
    finally:
        orchestrator.close()


@cli.command()
@click.pass_context
def run_bronze(ctx: click.Context) -> None:
    """Run Bronze Layer (Ingestion)."""
    db_path = ctx.obj["duckdb_path"]
    orchestrator = PipelineOrchestrator(db_path)
    try:
        orchestrator.run_bronze()
    finally:
        orchestrator.close()


@cli.command()
@click.pass_context
def run_silver(ctx: click.Context) -> None:
    """Run Silver Layer (Transformation)."""
    db_path = ctx.obj["duckdb_path"]
    orchestrator = PipelineOrchestrator(db_path)
    try:
        orchestrator.run_silver()
    finally:
        orchestrator.close()


@cli.command()
@click.pass_context
def run_gold(ctx: click.Context) -> None:
    """Run Gold Layer (Projection)."""
    db_path = ctx.obj["duckdb_path"]
    orchestrator = PipelineOrchestrator(db_path)
    try:
        orchestrator.run_gold()
    finally:
        orchestrator.close()


if __name__ == "__main__":
    cli()  # pragma: no cover
