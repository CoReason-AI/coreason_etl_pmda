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

from coreason_etl_pmda.pipeline_full import run_full_pipeline


@pytest.fixture  # type: ignore[misc]
def mock_dlt_pipeline() -> Generator[MagicMock, None, None]:
    with patch("coreason_etl_pmda.pipeline_full.run_bronze_pipeline") as mock:
        yield mock


def test_pipeline_idempotency(tmp_path: Path, mock_dlt_pipeline: MagicMock) -> None:
    """
    Verify that running the pipeline multiple times does not duplicate data in Gold tables.
    """
    db_path = tmp_path / "test_idempotency.duckdb"
    db_path_str = str(db_path)

    # 1. Setup Bronze Data
    con = duckdb.connect(db_path_str)
    con.execute("CREATE SCHEMA pmda_bronze")

    approvals_data = pl.DataFrame({
        "承認番号": ["1001"],
        "承認年月日": ["R2.1.1"],
        "販売名": ["Drug A"],
        "一般的名称": ["Generic A"],
        "申請者氏名": ["Co A"]
    })
    con.register("df_approvals", approvals_data)
    con.execute("CREATE TABLE pmda_bronze.bronze_approvals AS SELECT * FROM df_approvals")

    # Minimal JADER
    con.execute("CREATE TABLE pmda_bronze.bronze_jader_demo AS SELECT 'C1' as 識別番号, 'M' as 性別, '50' as 年齢, '2020' as 報告年度")
    con.execute('CREATE TABLE pmda_bronze.bronze_jader_drug AS SELECT \'C1\' as 識別番号, \'Drug A\' as "医薬品（一般名）", \'被疑薬\' as 被疑薬等区分')
    con.execute("CREATE TABLE pmda_bronze.bronze_jader_reac AS SELECT 'C1' as 識別番号, 'Reaction X' as 有害事象")

    # Minimal JAN
    con.execute("CREATE TABLE pmda_bronze.bronze_ref_jan_inn AS SELECT 'Generic A' as jan_name_jp, 'Generic A (EN)' as jan_name_en, 'Generic A (INN)' as inn_name_en")
    con.close()

    # 2. Run Pipeline Twice
    run_full_pipeline(duckdb_path=db_path_str)
    run_full_pipeline(duckdb_path=db_path_str)

    # 3. Verify Counts
    con = duckdb.connect(db_path_str)

    gold_approvals = con.execute("SELECT * FROM pmda_gold.pmda_approvals").pl()
    assert len(gold_approvals) == 1

    gold_jader = con.execute("SELECT * FROM pmda_gold.pmda_adverse_events").pl()
    assert len(gold_jader) == 1

    con.close()


def test_pipeline_schema_evolution(tmp_path: Path, mock_dlt_pipeline: MagicMock) -> None:
    """
    Verify robustness against extra or missing optional columns in Bronze.
    """
    db_path = tmp_path / "test_schema.duckdb"
    db_path_str = str(db_path)

    con = duckdb.connect(db_path_str)
    con.execute("CREATE SCHEMA pmda_bronze")

    # Bronze Approvals: MISSING '申請者氏名' (applicant), EXTRA 'ExtraCol'
    # Note: '申請者氏名' is mapped to 'applicant_name_jp'.
    # Silver transform should create null for missing, and ignore extra.
    approvals_data = pl.DataFrame({
        "承認番号": ["2001"],
        "承認年月日": ["R3.1.1"],
        "販売名": ["Drug B"],
        "一般的名称": ["Generic B"],
        # Missing: 申請者氏名
        "ExtraCol": ["ShouldBeIgnored"]
    })
    con.register("df_approvals", approvals_data)
    con.execute("CREATE TABLE pmda_bronze.bronze_approvals AS SELECT * FROM df_approvals")

    # Bronze JAN (Standard)
    con.execute("CREATE TABLE pmda_bronze.bronze_ref_jan_inn AS SELECT 'Generic B' as jan_name_jp, 'Gen B' as jan_name_en, 'Gen B' as inn_name_en")

    # Empty JADER to skip that part
    con.execute("CREATE TABLE pmda_bronze.bronze_jader_demo (識別番号 VARCHAR)")
    con.execute("CREATE TABLE pmda_bronze.bronze_jader_drug (識別番号 VARCHAR)")
    con.execute("CREATE TABLE pmda_bronze.bronze_jader_reac (識別番号 VARCHAR)")

    con.close()

    # Run Pipeline
    run_full_pipeline(duckdb_path=db_path_str)

    # Verify
    con = duckdb.connect(db_path_str)
    res = con.execute("SELECT * FROM pmda_gold.pmda_approvals").pl()

    assert len(res) == 1
    # Check that missing column is Null (not error)
    assert res["applicant_name_jp"][0] is None
    # Check that extra column is NOT present
    assert "ExtraCol" not in res.columns

    con.close()


def test_pipeline_jader_end_to_end_complex(tmp_path: Path, mock_dlt_pipeline: MagicMock) -> None:
    """
    Verify full flow from messy Japanese input (whitespace) to clean Gold output.
    """
    db_path = tmp_path / "test_jader_complex.duckdb"
    db_path_str = str(db_path)

    con = duckdb.connect(db_path_str)
    con.execute("CREATE SCHEMA pmda_bronze")

    # JADER with messy inputs
    # ID has spaces: " C1 "
    # Sex has spaces? " M " -> Normalize logic?
    # Drug characterization " 被疑薬 " (Suspected with spaces)

    # Demo
    con.execute('CREATE TABLE pmda_bronze.bronze_jader_demo AS SELECT \' C1 \' as "識別 番号", \' M \' as 性別, \'50\' as 年齢, \'2020\' as 報告年度')

    # Drug: 1 Suspected (Messy), 1 Concomitant
    # Note: Header "医薬品（一般名）" vs "医薬品 (一般名)"? Silver handles full width/normalization.
    # Source usually has full-width parens.
    drug_data = pl.DataFrame({
        "識別 番号": [" C1 ", " C1 "],
        "医薬品（一般名）": ["Drug A", "Drug B"],
        "被疑薬等区分": [" 被疑薬 ", " 併用薬 "] # Suspected (Messy), Concomitant
    })
    con.register("df_drug", drug_data)
    con.execute("CREATE TABLE pmda_bronze.bronze_jader_drug AS SELECT * FROM df_drug")

    # Reac
    con.execute('CREATE TABLE pmda_bronze.bronze_jader_reac AS SELECT \' C1 \' as "識別 番号", \'Reaction Z\' as 有害事象')

    # Empty Approvals/JAN
    con.execute("CREATE TABLE pmda_bronze.bronze_approvals (承認番号 VARCHAR)")
    con.execute("CREATE TABLE pmda_bronze.bronze_ref_jan_inn (jan_name_jp VARCHAR)")

    con.close()

    # Run Pipeline
    run_full_pipeline(duckdb_path=db_path_str)

    # Verify
    con = duckdb.connect(db_path_str)
    res = con.execute("SELECT * FROM pmda_gold.pmda_adverse_events").pl()

    assert len(res) == 1
    # Check ID normalized (trimmed)
    assert res["case_id"][0] == "C1"
    # Check Drug filtered correctly (only Suspected kept)
    assert res["primary_suspect_drug"][0] == "Drug A"

    con.close()
