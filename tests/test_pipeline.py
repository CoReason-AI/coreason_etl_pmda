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


def test_run_bronze_pipeline() -> None:
    with patch("coreason_etl_pmda.pipeline.dlt.pipeline") as mock_pipeline:
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance

        # Mock sources to be iterables
        with (
            patch("coreason_etl_pmda.pipeline.jan_inn_source", return_value=[]),
            patch("coreason_etl_pmda.pipeline.approvals_source", return_value=[]),
            patch("coreason_etl_pmda.pipeline.jader_source", return_value=[]),
        ):
            run_bronze_pipeline()

            mock_p_instance.run.assert_called()
