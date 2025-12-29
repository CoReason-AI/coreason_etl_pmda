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
from coreason_etl_pmda.sources_approvals import approvals_source


def test_approvals_source() -> None:
    # Mock request to main page
    html_content = """
    <html>
        <body>
            <a href="data_2024.xlsx">2024 Data</a>
            <a href="data_2023.xls">2023 Data</a>
            <a href="other.pdf">Guidance</a>
        </body>
    </html>
    """

    # Mock DataFrames
    df_2024 = pl.DataFrame({"id": [1], "name": ["Drug A"]})
    df_2023 = pl.DataFrame({"id": [2], "name": ["Drug B"]})

    with (
        patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_approvals.pl.read_excel") as mock_read_excel,
        patch("dlt.current.source_state", return_value={}),
    ):
        # Setup mock responses
        # First call is main page, subsequent are files
        def side_effect(url: str) -> MagicMock:
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            if url == "http://example.com/main":
                resp.content = html_content.encode("utf-8")
            elif url == "http://example.com/data_2024.xlsx":
                resp.content = b"excel2024"
            elif url == "http://example.com/data_2023.xls":
                resp.content = b"excel2023"
            else:
                resp.status_code = 404
                resp.raise_for_status.side_effect = Exception("404")
            return resp

        mock_get.side_effect = side_effect

        # Setup read_excel side effect
        # We need to match the content or just return based on call count/args?
        # Since we use BytesIO, we can check the bytes content if we want strictness.
        # But simpler to just return list of DFs.
        mock_read_excel.side_effect = [df_2024, df_2023]

        resource = approvals_source(url="http://example.com/main")
        data = list(resource)

        assert len(data) == 2
        assert data[0]["name"] == "Drug A"
        assert data[0]["_source_url"] == "http://example.com/data_2024.xlsx"
        assert data[1]["name"] == "Drug B"
        assert data[1]["_source_url"] == "http://example.com/data_2023.xls"


def test_approvals_source_state() -> None:
    # Test that visited URLs are skipped
    html_content = """
    <html>
        <body>
            <a href="data_2024.xlsx">2024 Data</a>
        </body>
    </html>
    """

    state = {"visited_urls": {"http://example.com/data_2024.xlsx": True}}

    with (
        patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_approvals.pl.read_excel") as mock_read_excel,
        patch("dlt.current.source_state", return_value=state),
    ):
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_get.return_value = mock_resp

        resource = approvals_source(url="http://example.com/main")
        data = list(resource)

        # Should be empty as it's visited
        assert len(data) == 0
        mock_read_excel.assert_not_called()


def test_approvals_source_error_handling() -> None:
    # One file fails, should continue?
    # Our code catches Exception and prints, so it should continue or finish empty.
    html_content = """
    <html>
        <body>
            <a href="bad.xlsx">Bad Data</a>
        </body>
    </html>
    """

    with (
        patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_approvals.pl.read_excel", side_effect=Exception("Corrupt")),
        patch("dlt.current.source_state", return_value={}),
    ):

        def side_effect(url: str) -> MagicMock:
            resp = MagicMock()
            if url == "http://example.com/main":
                resp.content = html_content.encode("utf-8")
            else:
                resp.content = b"bad"
            return resp

        mock_get.side_effect = side_effect

        resource = approvals_source(url="http://example.com/main")
        data = list(resource)

        assert len(data) == 0
