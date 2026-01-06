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

from coreason_etl_pmda.pipeline import run_bronze_pipeline


def test_run_bronze_pipeline_defaults() -> None:
    with patch("dlt.pipeline") as mock_pipeline:
        mock_p = MagicMock()
        mock_pipeline.return_value = mock_p
        mock_p.run.return_value = "info"

        info = run_bronze_pipeline()

        assert info == "info"
        mock_pipeline.assert_called_with(
            pipeline_name="coreason_etl_pmda_bronze",
            destination="duckdb",
            dataset_name="pmda_bronze",
        )


def test_run_bronze_pipeline_custom_path() -> None:
    with patch("dlt.pipeline") as mock_pipeline:
        mock_p = MagicMock()
        mock_pipeline.return_value = mock_p

        run_bronze_pipeline(duckdb_path="custom.db")

        mock_pipeline.assert_called_with(
            pipeline_name="coreason_etl_pmda_bronze",
            destination="duckdb:///custom.db",
            dataset_name="pmda_bronze",
        )


def test_run_bronze_pipeline_env_var() -> None:
    """Test that settings.DUCKDB_PATH is used if duckdb_path arg is not provided."""
    with patch("dlt.pipeline") as mock_pipeline:
        mock_p = MagicMock()
        mock_pipeline.return_value = mock_p

        # Patch the settings object imported in pipeline
        with patch("coreason_etl_pmda.pipeline.settings") as mock_settings:
            mock_settings.DUCKDB_PATH = "env.db"

            run_bronze_pipeline()

            mock_pipeline.assert_called_with(
                pipeline_name="coreason_etl_pmda_bronze",
                destination="duckdb:///env.db",
                dataset_name="pmda_bronze",
            )


def test_run_bronze_pipeline_custom_destination_obj() -> None:
    # If user passes a custom destination object (not string "duckdb")
    # duckdb_path should be ignored or irrelevant?
    custom_dest = MagicMock()

    with patch("dlt.pipeline") as mock_pipeline:
        mock_p = MagicMock()
        mock_pipeline.return_value = mock_p

        run_bronze_pipeline(destination=custom_dest, duckdb_path="ignored.db")

        # It shouldn't change destination
        mock_pipeline.assert_called_with(
            pipeline_name="coreason_etl_pmda_bronze",
            destination=custom_dest,
            dataset_name="pmda_bronze",
        )
