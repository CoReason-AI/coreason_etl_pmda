# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from coreason_etl_pmda.main import cli


def test_cli_run_all() -> None:
    runner = CliRunner()
    with patch("coreason_etl_pmda.main.PipelineOrchestrator") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        # Note: Options for the group must come BEFORE the subcommand
        result = runner.invoke(cli, ["--duckdb-path", "test.db", "run-all"])

        assert result.exit_code == 0
        mock_cls.assert_called_with("test.db")
        mock_instance.run_bronze.assert_called_once()
        mock_instance.run_silver.assert_called_once()
        mock_instance.run_gold.assert_called_once()
        mock_instance.close.assert_called_once()


def test_cli_run_bronze() -> None:
    runner = CliRunner()
    with patch("coreason_etl_pmda.main.PipelineOrchestrator") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["--duckdb-path", "test.db", "run-bronze"])

        assert result.exit_code == 0
        mock_instance.run_bronze.assert_called_once()
        mock_instance.run_silver.assert_not_called()
        mock_instance.run_gold.assert_not_called()
        mock_instance.close.assert_called_once()


def test_cli_run_silver() -> None:
    runner = CliRunner()
    with patch("coreason_etl_pmda.main.PipelineOrchestrator") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["run-silver"])

        assert result.exit_code == 0
        # Default path
        mock_cls.assert_called_with("pmda.duckdb")
        mock_instance.run_bronze.assert_not_called()
        mock_instance.run_silver.assert_called_once()
        mock_instance.run_gold.assert_not_called()
        mock_instance.close.assert_called_once()


def test_cli_run_gold() -> None:
    runner = CliRunner()
    with patch("coreason_etl_pmda.main.PipelineOrchestrator") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        result = runner.invoke(cli, ["run-gold"])

        assert result.exit_code == 0
        mock_instance.run_gold.assert_called_once()
        mock_instance.close.assert_called_once()


def test_cli_exception_handling() -> None:
    """Ensure close is called even if exception occurs."""
    runner = CliRunner()
    with patch("coreason_etl_pmda.main.PipelineOrchestrator") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        mock_instance.run_bronze.side_effect = RuntimeError("Fail")

        result = runner.invoke(cli, ["run-bronze"])

        assert result.exit_code != 0
        mock_instance.close.assert_called_once()
