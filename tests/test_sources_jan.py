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
from coreason_etl_pmda.sources_jan import jan_inn_source
from dlt.extract.exceptions import ResourceExtractionError


def test_jan_inn_source_direct_file() -> None:
    # Test direct file download (no HTML parsing needed)
    mock_df = pl.DataFrame({"jan_name_jp": ["A"], "jan_name_en": ["B"], "inn_name_en": ["B"]})

    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", return_value=mock_df) as mock_read_excel,
    ):
        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
        mock_response.content = b"fake excel"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        resource = jan_inn_source("http://example.com/file.xlsx")
        data = list(resource)

        assert len(data) == 1
        # Should only call get once
        assert mock_get.call_count == 1
        mock_read_excel.assert_called_once()


def test_jan_inn_source_html_scrape() -> None:
    # Test HTML scraping logic
    mock_df = pl.DataFrame({"jan_name_jp": ["A"], "jan_name_en": ["B"], "inn_name_en": ["B"]})

    html_content = """
    <html>
        <body>
            <a href="other.pdf">Guideline</a>
            <a href="data_2024.xlsx">List of JAN (Japanese Accepted Names)</a>
            <a href="old_data.csv">Old Data</a>
        </body>
    </html>
    """

    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", return_value=mock_df),
    ):
        # First call returns HTML, second call returns File
        mock_resp_html = MagicMock()
        mock_resp_html.headers = {"Content-Type": "text/html; charset=utf-8"}
        mock_resp_html.content = html_content.encode("utf-8")

        mock_resp_file = MagicMock()
        mock_resp_file.headers = {"Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
        mock_resp_file.content = b"fake excel"

        mock_get.side_effect = [mock_resp_html, mock_resp_file]

        resource = jan_inn_source("http://example.com/page")
        data = list(resource)

        assert len(data) == 1
        assert mock_get.call_count == 2
        # Verify second call was to the link
        assert mock_get.call_args_list[1][0][0] == "http://example.com/data_2024.xlsx"


def test_jan_inn_source_html_no_link() -> None:
    # Test HTML with no suitable links
    html_content = "<html><body><a href='other.pdf'>Guideline</a></body></html>"

    with patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get:
        mock_resp_html = MagicMock()
        mock_resp_html.headers = {"Content-Type": "text/html"}
        mock_resp_html.content = html_content.encode("utf-8")
        mock_get.return_value = mock_resp_html

        resource = jan_inn_source()

        with pytest.raises(ResourceExtractionError) as excinfo:
            list(resource)
        assert "No suitable Excel/CSV link found" in str(excinfo.value)


def test_jan_inn_source_duplicate_mapping_handled() -> None:
    # Test that duplicate mapping doesn't crash
    mock_df = pl.DataFrame(
        {
            "JAN（日本名）": ["Full"],
            "JAN(日本名)": ["Half"],
        }
    )

    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", return_value=mock_df),
    ):
        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
        mock_response.content = b"fake content"
        mock_get.return_value = mock_response

        resource = jan_inn_source()
        data = list(resource)

        # Should succeed and have one target column
        assert len(data) == 1
        row = data[0]
        assert "jan_name_jp" in row


def test_jan_inn_source_normalization() -> None:
    # Verify that Japanese headers are correctly renamed
    mock_df = pl.DataFrame(
        {
            "JAN（日本名）": ["アスピリン"],
            "JAN（英名）": ["Aspirin"],
            "INN": ["Aspirin"],
            "Extra": ["Ignore"],
        }
    )

    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", return_value=mock_df),
    ):
        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": "application/excel"}
        mock_response.content = b"fake content"
        mock_get.return_value = mock_response

        resource = jan_inn_source()
        data = list(resource)

        assert len(data) == 1
        row = data[0]
        assert row["jan_name_jp"] == "アスピリン"
        assert "JAN（日本名）" not in row


def test_jan_inn_source_csv_fallback() -> None:
    mock_df = pl.DataFrame({"jan_name_jp": ["A"]})

    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", side_effect=Exception("Not Excel")),
        patch("coreason_etl_pmda.sources_jan.pl.read_csv", return_value=mock_df) as mock_read_csv,
    ):
        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": "application/csv"}
        mock_response.content = b"fake csv"
        mock_get.return_value = mock_response

        resource = jan_inn_source()
        data = list(resource)

        assert len(data) == 1
        mock_read_csv.assert_called_once()


def test_jan_inn_source_parsing_failure() -> None:
    # Test failure of both Excel and CSV parsing
    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", side_effect=Exception("Not Excel")),
        patch("coreason_etl_pmda.sources_jan.pl.read_csv", side_effect=Exception("Not CSV")),
    ):
        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": "application/octet-stream"}
        mock_response.content = b"garbage"
        mock_get.return_value = mock_response

        resource = jan_inn_source()

        with pytest.raises(ResourceExtractionError) as excinfo:
            list(resource)
        assert "Could not parse JAN/INN file" in str(excinfo.value)


def test_jan_inn_source_missing_required_column() -> None:
    # Test warning if jan_name_jp is missing
    mock_df = pl.DataFrame({"wrong_column": ["A"]})

    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", return_value=mock_df),
        patch("coreason_etl_pmda.sources_jan.logger.warning") as mock_warn,
    ):
        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": "application/excel"}
        mock_response.content = b"fake content"
        mock_get.return_value = mock_response

        resource = jan_inn_source()
        data = list(resource)

        assert len(data) == 1
        mock_warn.assert_called_once()
        assert "jan_name_jp not found" in mock_warn.call_args[0][0]
