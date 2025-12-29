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
from unittest.mock import patch

import duckdb
import polars as pl
import pytest

from coreason_etl_pmda.pipeline_full import run_full_pipeline

# Sample Data
BRONZE_APPROVALS = pl.DataFrame(
    {
        "approval_id": ["123"],
        "approval_date": ["Reiwa 2.5.1"],
        "brand_name_jp": ["Brand A"],
        "generic_name_jp": ["Generic A"],
        "applicant_name_jp": ["Applicant X"],
        "review_report_url": ["http://example.com/pdf"],
    }
)

BRONZE_JAN = pl.DataFrame(
    {
        "jan_name_jp": ["Generic A"],
        "jan_name_en": ["Generic A (JAN)"],
        "inn_name_en": ["Generic A (INN)"],
    }
)

BRONZE_JADER_DEMO = pl.DataFrame(
    {
        "識別番号": ["case1"],
        "性別": ["Male"],
        "年齢": ["30"],
        "報告年度": ["2020"],
    }
)

BRONZE_JADER_DRUG = pl.DataFrame(
    {
        "識別番号": ["case1"],
        "医薬品（一般名）": ["Drug D"],
        "被疑薬等区分": ["被疑薬"],  # Suspected
    }
)

BRONZE_JADER_REAC = pl.DataFrame(
    {
        "識別番号": ["case1"],
        "有害事象": ["Reaction R"],
    }
)


@pytest.fixture
def temp_duckdb(tmp_path):
    db_file = tmp_path / "test_pmda.duckdb"
    return str(db_file)


def test_pipeline_full_e2e(temp_duckdb):
    """
    Tests the full pipeline orchestration.
    Mocks Bronze ingestion by pre-populating the DuckDB.
    Verifies Silver and Gold tables are created.
    """

    # 1. Setup Environment
    os.environ["DUCKDB_PATH"] = temp_duckdb

    # Populate DB
    con = duckdb.connect(temp_duckdb)
    con.register("BRONZE_APPROVALS", BRONZE_APPROVALS)
    con.execute("CREATE TABLE bronze_approvals AS SELECT * FROM BRONZE_APPROVALS")

    con.register("BRONZE_JAN", BRONZE_JAN)
    con.execute("CREATE TABLE bronze_ref_jan_inn AS SELECT * FROM BRONZE_JAN")

    con.register("BRONZE_JADER_DEMO", BRONZE_JADER_DEMO)
    con.execute("CREATE TABLE bronze_jader_demo AS SELECT * FROM BRONZE_JADER_DEMO")

    con.register("BRONZE_JADER_DRUG", BRONZE_JADER_DRUG)
    con.execute("CREATE TABLE bronze_jader_drug AS SELECT * FROM BRONZE_JADER_DRUG")

    con.register("BRONZE_JADER_REAC", BRONZE_JADER_REAC)
    con.execute("CREATE TABLE bronze_jader_reac AS SELECT * FROM BRONZE_JADER_REAC")
    con.close()

    # We assume 'run_bronze_pipeline' is imported in pipeline_full.py
    # We patch it to avoid dlt runs.
    with patch("coreason_etl_pmda.pipeline_full.run_bronze_pipeline") as mock_bronze:
        # Run Pipeline
        run_full_pipeline()

        assert mock_bronze.called

    # Verify Output
    con = duckdb.connect(temp_duckdb)
    tables = [x[0] for x in con.execute("SHOW TABLES").fetchall()]

    # Check Silver Tables
    assert "silver_approvals" in tables
    assert "silver_jader_demo" in tables

    # Check Gold Tables
    assert "gold_pmda_approvals" in tables
    assert "gold_pmda_adverse_events" in tables

    # Verify Content
    # Gold Approvals
    df_gold_app = pl.read_database("SELECT * FROM gold_pmda_approvals", con)
    assert len(df_gold_app) == 1
    row = df_gold_app.row(0, named=True)
    assert row["approval_date"] == "2020-05-01"  # Normalized
    assert row["generic_name_en"] == "Generic A (INN)"  # From JAN Bridge

    # Gold JADER
    df_gold_jader = pl.read_database("SELECT * FROM gold_pmda_adverse_events", con)
    assert len(df_gold_jader) == 1
    row_j = df_gold_jader.row(0, named=True)
    assert row_j["case_id"] == "case1"
    assert row_j["primary_suspect_drug"] == "Drug D"

    con.close()


def test_pipeline_missing_keys(temp_duckdb):
    """
    Test fallback when API key is missing (AI skipped).
    """
    os.environ["DUCKDB_PATH"] = temp_duckdb
    # Ensure no API Key
    if "DEEPSEEK_API_KEY" in os.environ:
        del os.environ["DEEPSEEK_API_KEY"]

    # Populate DB with approval needing translation (no JAN match)
    con = duckdb.connect(temp_duckdb)

    # Approval with NO matching JAN
    df_app = pl.DataFrame(
        {
            "approval_id": ["999"],
            "approval_date": ["Reiwa 2.1.1"],
            "brand_name_jp": ["Brand Z"],
            "generic_name_jp": ["Unknown Generic"],  # No match in JAN
            "applicant_name_jp": ["App Z"],
            "review_report_url": ["url"],
        }
    )

    con.register("df_app", df_app)
    con.execute("CREATE TABLE bronze_approvals AS SELECT * FROM df_app")

    # FIX: Insert a dummy row so Polars infers schema correctly (String), instead of empty table (Null)
    con.execute("CREATE TABLE bronze_ref_jan_inn (jan_name_jp VARCHAR, jan_name_en VARCHAR, inn_name_en VARCHAR)")
    con.execute("INSERT INTO bronze_ref_jan_inn VALUES ('dummy', 'dummy', 'dummy')")

    con.close()

    with patch("coreason_etl_pmda.pipeline_full.run_bronze_pipeline"):
        run_full_pipeline()

    con = duckdb.connect(temp_duckdb)
    df_gold = pl.read_database("SELECT * FROM gold_pmda_approvals", con)

    assert len(df_gold) == 1
    row = df_gold.row(0, named=True)
    assert row["generic_name_en"] is None  # Should be null because AI skipped and no lookup match (dummy doesn't match)

    # Check Silver for status
    df_silver = pl.read_database("SELECT * FROM silver_approvals", con)
    assert "_translation_status" in df_silver.columns
    assert df_silver["_translation_status"][0] == "skipped_no_key"

    con.close()


def test_pipeline_empty_db(temp_duckdb):
    """
    Test pipeline runs gracefully when Bronze tables are missing.
    Should skip Silver/Gold steps with warnings.
    """
    os.environ["DUCKDB_PATH"] = temp_duckdb

    # Empty DB
    con = duckdb.connect(temp_duckdb)
    con.close()

    with patch("coreason_etl_pmda.pipeline_full.run_bronze_pipeline"):
        run_full_pipeline()

    # Check that execution finished without error
    # And Silver/Gold tables are NOT created (or created empty? Code says skip)
    con = duckdb.connect(temp_duckdb)
    tables = [x[0] for x in con.execute("SHOW TABLES").fetchall()]

    assert "silver_approvals" not in tables
    assert "gold_pmda_approvals" not in tables

    con.close()


def test_pipeline_partial_tables(temp_duckdb):
    """
    Test missing JAN table (lookup skip) and missing some JADER tables.
    """
    os.environ["DUCKDB_PATH"] = temp_duckdb

    con = duckdb.connect(temp_duckdb)
    con.register("BRONZE_APPROVALS", BRONZE_APPROVALS)
    con.execute("CREATE TABLE bronze_approvals AS SELECT * FROM BRONZE_APPROVALS")
    # Missing JAN
    # Missing JADER
    con.close()

    with patch("coreason_etl_pmda.pipeline_full.run_bronze_pipeline"):
        run_full_pipeline()

    con = duckdb.connect(temp_duckdb)
    tables = [x[0] for x in con.execute("SHOW TABLES").fetchall()]

    assert "silver_approvals" in tables

    # Check content: JAN lookup skipped
    df = pl.read_database("SELECT * FROM silver_approvals", con)
    # generic_name_en should be None (created as null)
    assert df["generic_name_en"][0] is None

    assert "silver_jader_demo" not in tables

    con.close()


def test_pipeline_exception_approvals(temp_duckdb):
    """
    Test that exceptions in Silver Approvals are caught and re-raised.
    """
    os.environ["DUCKDB_PATH"] = temp_duckdb
    con = duckdb.connect(temp_duckdb)
    con.register("BRONZE_APPROVALS", BRONZE_APPROVALS)
    con.execute("CREATE TABLE bronze_approvals AS SELECT * FROM BRONZE_APPROVALS")
    con.close()

    with patch("coreason_etl_pmda.pipeline_full.normalize_approvals", side_effect=Exception("Boom!")):
        with pytest.raises(Exception, match="Boom!"):
            with patch("coreason_etl_pmda.pipeline_full.run_bronze_pipeline"):
                run_full_pipeline()


def test_pipeline_exception_jader(temp_duckdb):
    """
    Test that exceptions in Silver JADER are caught and re-raised.
    """
    os.environ["DUCKDB_PATH"] = temp_duckdb
    con = duckdb.connect(temp_duckdb)
    con.register("BRONZE_APPROVALS", BRONZE_APPROVALS)
    con.execute("CREATE TABLE bronze_approvals AS SELECT * FROM BRONZE_APPROVALS")
    con.register("BRONZE_JAN", BRONZE_JAN)
    con.execute("CREATE TABLE bronze_ref_jan_inn AS SELECT * FROM BRONZE_JAN")  # JAN ok
    # Create JADER tables
    con.register("BRONZE_JADER_DEMO", BRONZE_JADER_DEMO)
    con.execute("CREATE TABLE bronze_jader_demo AS SELECT * FROM BRONZE_JADER_DEMO")
    con.register("BRONZE_JADER_DRUG", BRONZE_JADER_DRUG)
    con.execute("CREATE TABLE bronze_jader_drug AS SELECT * FROM BRONZE_JADER_DRUG")
    con.register("BRONZE_JADER_REAC", BRONZE_JADER_REAC)
    con.execute("CREATE TABLE bronze_jader_reac AS SELECT * FROM BRONZE_JADER_REAC")
    con.close()

    with patch("coreason_etl_pmda.pipeline_full.normalize_jader_demo", side_effect=Exception("JADER Boom!")):
        with pytest.raises(Exception, match="JADER Boom!"):
            with patch("coreason_etl_pmda.pipeline_full.run_bronze_pipeline"):
                run_full_pipeline()


def test_pipeline_with_api_key(temp_duckdb):
    """
    Test pipeline with DEEPSEEK_API_KEY present (AI Fallback path).
    """
    os.environ["DUCKDB_PATH"] = temp_duckdb
    os.environ["DEEPSEEK_API_KEY"] = "dummy_key"

    con = duckdb.connect(temp_duckdb)
    con.register("BRONZE_APPROVALS", BRONZE_APPROVALS)
    con.execute("CREATE TABLE bronze_approvals AS SELECT * FROM BRONZE_APPROVALS")
    con.register("BRONZE_JAN", BRONZE_JAN)
    con.execute("CREATE TABLE bronze_ref_jan_inn AS SELECT * FROM BRONZE_JAN")
    con.close()

    with patch("coreason_etl_pmda.pipeline_full.run_bronze_pipeline"):
        with patch("coreason_etl_pmda.pipeline_full.jan_bridge_ai_fallback") as mock_ai:
            # Mock return value to be the same dataframe (modified)
            # We don't need actual modification, just need to see if it's called
            def side_effect(df):
                return df.with_columns(pl.lit("ai_translated").alias("_translation_status"))

            mock_ai.side_effect = side_effect

            run_full_pipeline()

            assert mock_ai.called

    # Cleanup env
    del os.environ["DEEPSEEK_API_KEY"]
