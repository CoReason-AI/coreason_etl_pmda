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

import polars as pl
import pytest
from coreason_etl_pmda.silver.transform_silver_jader import normalize_jader_demo
from coreason_etl_pmda.sources.approvals import approvals_source
from pydantic import ValidationError


def test_silver_validation_failure() -> None:
    """Test that validation fails for data incompatible with schema."""
    # reporting_year expected to be convertible to int
    df = pl.DataFrame(
        {
            "識別番号": ["1"],
            "報告年度": ["NotAYear"],  # Invalid
        }
    )

    with pytest.raises(ValidationError):
        normalize_jader_demo(df)


def test_approvals_source_no_tables() -> None:
    """Test scraping where no tables are found."""
    html_content = "<html><body><p>No tables here</p></body></html>"
    with patch("coreason_etl_pmda.sources.common.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 0


def test_approvals_source_malformed_headers() -> None:
    """Test tables with weird headers or missing headers."""
    html_content = """
    <html>
        <body>
            <!-- Empty header row -->
            <table><tr></tr></table>
            <!-- Header row with no text -->
            <table><tr><th></th></tr></table>
            <!-- Header row with less cols than data -->
            <table>
                <tr><th>販売名</th></tr>
                <tr><td>Brand</td><td>Extra</td></tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources.common.fetch_url") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 0
