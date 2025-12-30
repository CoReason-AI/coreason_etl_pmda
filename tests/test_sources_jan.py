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


def test_jan_inn_source_excel() -> None:
    # Mock content as Excel file
    # We create a simple excel file in memory using openpyxl or just mock pl.read_excel return value?
    # It is safer to mock pl.read_excel to avoid needing valid binary excel content in test code.

    mock_df = pl.DataFrame({"jan_name_jp": ["アスピリン"], "jan_name_en": ["Aspirin"], "inn_name_en": ["Aspirin"]})

    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", return_value=mock_df) as mock_read_excel,
    ):
        mock_response = MagicMock()
        mock_response.content = b"fake excel content"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Run the source
        # dlt sources are iterables or callables returning iterables.
        # jan_inn_source is a @dlt.resource, so calling it returns a resource.
        # We can iterate over it.

        resource = jan_inn_source()
        data = list(resource)

        assert len(data) == 1
        assert data[0]["jan_name_jp"] == "アスピリン"
        mock_read_excel.assert_called_once()


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
        mock_response.content = b"fake content"
        mock_get.return_value = mock_response

        resource = jan_inn_source()
        data = list(resource)

        assert len(data) == 1
        row = data[0]
        # Check renames
        assert row["jan_name_jp"] == "アスピリン"
        assert row["jan_name_en"] == "Aspirin"
        assert row["inn_name_en"] == "Aspirin"
        # Check that original Japanese column names are NOT present (renamed)
        assert "JAN（日本名）" not in row
        # Extra column remains (or we could choose to drop it, but implementation keeps it)
        assert "Extra" in row


def test_jan_inn_source_missing_jan_name_jp() -> None:
    # Verify warning if jan_name_jp missing
    mock_df = pl.DataFrame({"WrongHeader": ["Value"]})

    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", return_value=mock_df),
        patch("coreason_etl_pmda.sources_jan.logger.warning") as mock_warn,
    ):
        mock_response = MagicMock()
        mock_response.content = b"fake content"
        mock_get.return_value = mock_response

        resource = jan_inn_source()
        data = list(resource)

        # Should yield rows even if warned
        assert len(data) == 1
        mock_warn.assert_called_once()


def test_jan_inn_source_csv_fallback() -> None:
    # Mock read_excel failure, succeed read_csv
    mock_df = pl.DataFrame(
        {
            "jan_name_jp": ["アスピリン"],
        }
    )

    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", side_effect=Exception("Not Excel")),
        patch("coreason_etl_pmda.sources_jan.pl.read_csv", return_value=mock_df) as mock_read_csv,
    ):
        mock_response = MagicMock()
        mock_response.content = b"fake csv content"
        mock_get.return_value = mock_response

        resource = jan_inn_source()
        data = list(resource)

        assert len(data) == 1
        assert data[0]["jan_name_jp"] == "アスピリン"
        mock_read_csv.assert_called_once()


def test_jan_inn_source_failure() -> None:
    # Mock both failing
    with (
        patch("coreason_etl_pmda.sources_jan.requests.get") as mock_get,
        patch("coreason_etl_pmda.sources_jan.pl.read_excel", side_effect=Exception("Not Excel")),
        patch("coreason_etl_pmda.sources_jan.pl.read_csv", side_effect=Exception("Not CSV")),
    ):
        mock_response = MagicMock()
        mock_response.content = b"garbage"
        mock_get.return_value = mock_response

        resource = jan_inn_source()
        # dlt wraps exceptions in ResourceExtractionError, so we check for that or unpack it.
        # But we also want to verify the inner exception message.
        from dlt.extract.exceptions import ResourceExtractionError

        with pytest.raises(ResourceExtractionError) as excinfo:
            list(resource)
        assert "Could not parse JAN/INN file" in str(excinfo.value)
