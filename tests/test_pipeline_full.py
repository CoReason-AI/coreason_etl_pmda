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
from typing import Generator
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pytest
from coreason_etl_pmda.pipeline_full import PipelineOrchestrator, run_full_pipeline


@pytest.fixture  # type: ignore[misc]
def mock_dlt_pipeline() -> Generator[MagicMock, None, None]:
    with patch("coreason_etl_pmda.pipeline_full.run_bronze_pipeline") as mock:
        yield mock


def test_full_pipeline_orchestration(tmp_path: Path, mock_dlt_pipeline: MagicMock) -> None:
    """
    Tests the full pipeline orchestration (Happy Path).
    """
    db_path = tmp_path / "test_pipeline.duckdb"
    db_path_str = str(db_path)

    # 1. Setup Bronze Data manually
    con = duckdb.connect(db_path_str)
    con.execute("CREATE SCHEMA pmda_bronze")

    # Approvals
    approvals_data = pl.DataFrame(
        {
            "承認番号": ["123"],
            "承認年月日": ["R2.1.1"],
            "販売名": ["DrugA"],
            "一般的名称": ["GenericA"],
            "申請者氏名": ["ApplicantA"],
        }
    )
    con.register("df_approvals", approvals_data)
    con.execute("CREATE TABLE pmda_bronze.bronze_approvals AS SELECT * FROM df_approvals")

    # JAN
    jan_data = pl.DataFrame(
        {"jan_name_jp": ["GenericA"], "jan_name_en": ["GenericA (JAN)"], "inn_name_en": ["GenericA (INN)"]}
    )
    con.register("df_jan", jan_data)
    con.execute("CREATE TABLE pmda_bronze.bronze_ref_jan_inn AS SELECT * FROM df_jan")

    # JADER
    demo_data = pl.DataFrame({"識別番号": ["C1"], "性別": ["Male"], "年齢": ["50"], "報告年度": ["2020"]})
    drug_data = pl.DataFrame(
        {
            "識別番号": ["C1"],
            "医薬品（一般名）": ["DrugA"],
            "被疑薬等区分": ["被疑薬"],  # Suspected
        }
    )
    reac_data = pl.DataFrame({"識別番号": ["C1"], "有害事象": ["ReactionX"]})

    con.register("df_demo", demo_data)
    con.register("df_drug", drug_data)
    con.register("df_reac", reac_data)

    con.execute("CREATE TABLE pmda_bronze.bronze_jader_demo AS SELECT * FROM df_demo")
    con.execute("CREATE TABLE pmda_bronze.bronze_jader_drug AS SELECT * FROM df_drug")
    con.execute("CREATE TABLE pmda_bronze.bronze_jader_reac AS SELECT * FROM df_reac")

    con.close()

    # 2. Run Pipeline
    run_full_pipeline(duckdb_path=db_path_str)

    # 3. Verify
    con = duckdb.connect(db_path_str)
    res_approvals = con.execute("SELECT * FROM pmda_gold.pmda_approvals").pl()
    assert res_approvals.height == 1
    assert res_approvals["generic_name_en"][0] == "GenericA (INN)"
    con.close()


def test_pipeline_missing_tables(tmp_path: Path, mock_dlt_pipeline: MagicMock) -> None:
    """Test running Silver/Gold when Bronze tables are missing (should skip safely)."""
    db_path = tmp_path / "test_missing.duckdb"

    # Create empty DB
    # We call orchestration directly to inspect logs or just ensure no crash
    run_full_pipeline(duckdb_path=str(db_path))

    con = duckdb.connect(str(db_path))
    schemas = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    schema_names = [s[0] for s in schemas]
    # Expect pmda_gold to be created immediately in run_gold, but pmda_silver might depend
    # run_silver now creates schema at start.
    assert "pmda_silver" in schema_names
    assert "pmda_gold" in schema_names
    con.close()


def test_pipeline_missing_jan_data(tmp_path: Path, mock_dlt_pipeline: MagicMock) -> None:
    """Test Silver Approvals when JAN table is empty (skips lookup)."""
    db_path = tmp_path / "test_no_jan.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA pmda_bronze")

    # Approvals with data
    con.execute(
        "CREATE TABLE pmda_bronze.bronze_approvals "
        "(承認番号 VARCHAR, 承認年月日 VARCHAR, 販売名 VARCHAR, 一般的名称 VARCHAR, 申請者氏名 VARCHAR)"
    )
    con.execute("INSERT INTO pmda_bronze.bronze_approvals VALUES ('1', 'R2.1.1', 'Brand', 'Generic', 'App')")

    # JAN empty
    con.execute(
        "CREATE TABLE pmda_bronze.bronze_ref_jan_inn (jan_name_jp VARCHAR, jan_name_en VARCHAR, inn_name_en VARCHAR)"
    )

    con.close()

    run_full_pipeline(duckdb_path=str(db_path))

    con = duckdb.connect(str(db_path))
    res = con.execute("SELECT * FROM pmda_silver.silver_approvals").pl()
    assert res.height == 1
    # generic_name_en should be null
    assert res["generic_name_en"][0] is None
    con.close()


def test_pipeline_ai_fallback(tmp_path: Path, mock_dlt_pipeline: MagicMock) -> None:
    """Test AI Fallback trigger."""
    db_path = tmp_path / "test_ai.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA pmda_bronze")

    # Approvals with data
    con.execute(
        "CREATE TABLE pmda_bronze.bronze_approvals "
        "(承認番号 VARCHAR, 承認年月日 VARCHAR, 販売名 VARCHAR, 一般的名称 VARCHAR, 申請者氏名 VARCHAR)"
    )
    con.execute("INSERT INTO pmda_bronze.bronze_approvals VALUES ('1', 'R2.1.1', 'Brand', 'Generic', 'App')")
    con.execute(
        "CREATE TABLE pmda_bronze.bronze_ref_jan_inn (jan_name_jp VARCHAR, jan_name_en VARCHAR, inn_name_en VARCHAR)"
    )
    con.close()

    # Mock AI
    with patch("coreason_etl_pmda.pipeline_full.jan_bridge_ai_fallback") as mock_ai:
        mock_ai.side_effect = lambda df: df.with_columns(pl.lit("AI_Translated").alias("generic_name_en"))

        with patch.dict(os.environ, {"DEEPSEEK_API_KEY": "fake_key"}):
            run_full_pipeline(duckdb_path=str(db_path))

        assert mock_ai.called

    con = duckdb.connect(str(db_path))
    res = con.execute("SELECT * FROM pmda_silver.silver_approvals").pl()
    assert res["generic_name_en"][0] == "AI_Translated"
    con.close()


def test_pipeline_partial_jader(tmp_path: Path, mock_dlt_pipeline: MagicMock) -> None:
    """Test Gold JADER skip when Silver JADER is incomplete."""
    db_path = tmp_path / "test_partial.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA pmda_bronze")
    # Only Demo
    con.execute(
        "CREATE TABLE pmda_bronze.bronze_jader_demo (識別番号 VARCHAR, 性別 VARCHAR, 年齢 VARCHAR, 報告年度 VARCHAR)"
    )
    con.execute("INSERT INTO pmda_bronze.bronze_jader_demo VALUES ('C1', 'M', '50', '2020')")

    # Empty others
    con.execute(
        "CREATE TABLE pmda_bronze.bronze_jader_drug (識別番号 VARCHAR, 医薬品（一般名） VARCHAR, 被疑薬等区分 VARCHAR)"
    )
    con.execute("CREATE TABLE pmda_bronze.bronze_jader_reac (識別番号 VARCHAR, 有害事象 VARCHAR)")
    con.close()

    run_full_pipeline(duckdb_path=str(db_path))

    con = duckdb.connect(str(db_path))

    # Now explicitly test run_gold logic when Silver tables EXIST but are EMPTY
    # We want to force `silver_jader_drug` to exist but be EMPTY to hit the `run_gold` condition.
    con.execute("CREATE SCHEMA IF NOT EXISTS pmda_silver")
    con.execute("CREATE TABLE IF NOT EXISTS pmda_silver.silver_jader_demo AS SELECT 'C1' as id")
    con.execute("CREATE TABLE IF NOT EXISTS pmda_silver.silver_jader_drug (id VARCHAR, drug_name VARCHAR)")
    con.execute("CREATE TABLE IF NOT EXISTS pmda_silver.silver_jader_reac (id VARCHAR, reaction VARCHAR)")
    con.close()

    # Run ONLY gold
    orchestrator = PipelineOrchestrator(str(db_path))
    orchestrator.run_gold()
    orchestrator.close()

    # Should skip Gold JADER (and log warning)
    con = duckdb.connect(str(db_path))
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'pmda_gold'").fetchall()
    table_names = [t[0] for t in tables]
    assert "pmda_adverse_events" not in table_names
    con.close()


def test_write_failure(tmp_path: Path) -> None:
    """Test write failure handling by patching duckdb connection."""
    db_path = tmp_path / "test_fail.duckdb"

    # Mock DuckDB connection
    with patch("duckdb.connect") as mock_connect:
        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        # We need mock_con.sql(...).pl() to return a DataFrame for the reads
        # So we mock the read chain
        mock_pl = MagicMock()
        mock_pl.height = 1
        mock_con.sql.return_value.pl.return_value = mock_pl

        orchestrator = PipelineOrchestrator(str(db_path))

        # Patch execute to raise error on WRITE
        def side_effect(query: str) -> MagicMock:
            if "CREATE OR REPLACE TABLE pmda_silver.silver_approvals" in query:
                raise RuntimeError("Disk Full")
            # For other queries, return mock
            return MagicMock()

        mock_con.execute.side_effect = side_effect

        # We also need to mock `normalize_approvals` etc. so they don't fail on mock input
        with patch("coreason_etl_pmda.pipeline_full.normalize_approvals") as mock_norm:
            mock_norm.return_value = pl.DataFrame({"a": [1]})
            with patch("coreason_etl_pmda.pipeline_full.jan_bridge_lookup") as mock_jan:
                mock_jan.return_value = pl.DataFrame({"a": [1]})

                with pytest.raises(RuntimeError, match="Disk Full"):
                    orchestrator.run_silver()


def test_pipeline_bronze_missing_error(tmp_path: Path) -> None:
    """Test explicit error catching for missing tables."""
    db_path = tmp_path / "test_error.duckdb"

    with patch("duckdb.connect") as mock_connect:
        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        # Make .sql() raise duckdb.Error
        # This will be called in _run_silver_approvals first
        mock_con.sql.side_effect = duckdb.Error("Catalog Error")

        orchestrator = PipelineOrchestrator(str(db_path))

        # Should catch and log warning, not raise
        orchestrator._run_silver_approvals()

        # Verify it handled it (mock_con.sql called)
        assert mock_con.sql.called


def test_pipeline_bronze_empty_table(tmp_path: Path) -> None:
    """Test safe return when Bronze table exists but is empty."""
    db_path = tmp_path / "test_empty_table.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA pmda_bronze")

    # Create empty table
    con.execute(
        "CREATE TABLE pmda_bronze.bronze_approvals "
        "(承認番号 VARCHAR, 承認年月日 VARCHAR, 販売名 VARCHAR, 一般的名称 VARCHAR, 申請者氏名 VARCHAR)"
    )
    con.execute(
        "CREATE TABLE pmda_bronze.bronze_ref_jan_inn (jan_name_jp VARCHAR, jan_name_en VARCHAR, inn_name_en VARCHAR)"
    )
    con.close()

    run_full_pipeline(duckdb_path=str(db_path))

    con = duckdb.connect(str(db_path))
    # Should have skipped processing
    # pmda_silver schema exists (created at start of run_silver)
    # but silver_approvals table should NOT exist
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'pmda_silver'"
    ).fetchall()
    table_names = [t[0] for t in tables]
    assert "silver_approvals" not in table_names
    con.close()
