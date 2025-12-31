# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import io
import zipfile
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from coreason_etl_pmda.sources_jader import jader_source
from dlt.extract.exceptions import ResourceExtractionError
from dlt.sources.helpers import requests


def create_zip_with_csvs(files: dict[str, str | bytes]) -> bytes:
    """Helper to create a zip file in memory containing given files."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, content in files.items():
            if isinstance(content, str):
                z.writestr(name, content)
            else:
                z.writestr(name, content)
    return buf.getvalue()


def test_jader_source_scraping_and_extraction() -> None:
    """Test full flow: scrape page, download zip, extract CSVs."""
    html_content = """
    <html>
        <body>
            <a href="data_2020.zip">Data 2020</a>
            <a href="info.pdf">Info</a>
        </body>
    </html>
    """

    # Mock CSV content
    demo_csv = "ID,Sex,Age\n1,M,20"
    drug_csv = "ID,DrugName\n1,DrugA"
    reac_csv = "ID,Reaction\n1,Fever"

    zip_bytes = create_zip_with_csvs(
        {"demo2020.csv": demo_csv, "drug2020.csv": drug_csv, "reac2020.csv": reac_csv, "readme.txt": "ignore me"}
    )

    # We patch dlt.mark.with_table_name to inject a verification key
    # because the real dlt.mark.with_table_name might use internal metadata not easily accessible in raw dicts.
    def mock_with_table_name(item: dict[str, Any], table_name: str) -> dict[str, Any]:
        item["__mock_table_name"] = table_name
        return item

    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:
        with patch("coreason_etl_pmda.sources_jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            # Side effect for multiple calls: first page, then zip
            mock_page_resp = MagicMock()
            mock_page_resp.content = html_content.encode("utf-8")
            mock_page_resp.raise_for_status.return_value = None

            mock_zip_resp = MagicMock()
            mock_zip_resp.content = zip_bytes
            mock_zip_resp.raise_for_status.return_value = None

            def side_effect(url: str, *args: Any, **kwargs: Any) -> MagicMock:
                if url.endswith(".html"):
                    return mock_page_resp
                elif url.endswith(".zip"):
                    return mock_zip_resp
                return MagicMock()

            mock_get.side_effect = side_effect

            resource = jader_source(url="http://example.com/index.html")
            data = list(resource)

            # We expect 3 rows (1 from each csv)
            assert len(data) == 3

            # Check table routing
            tables = {d["__mock_table_name"] for d in data}
            assert tables == {"bronze_jader_demo", "bronze_jader_drug", "bronze_jader_reac"}

            # Check content
            for row in data:
                tname = row["__mock_table_name"]
                if tname == "bronze_jader_demo":
                    assert row["ID"] == "1"
                    assert row["Sex"] == "M"
                    assert row["_source_file"] == "demo2020.csv"
                elif tname == "bronze_jader_drug":
                    assert row["DrugName"] == "DrugA"
                elif tname == "bronze_jader_reac":
                    assert row["Reaction"] == "Fever"

                assert row["_source_zip"] == "http://example.com/data_2020.zip"
                assert "_ingestion_ts" in row


def test_jader_source_cp932_encoding() -> None:
    """Test handling of CP932 encoded CSVs."""
    html_content = """<a href="data.zip">Data</a>"""

    # "日本語" in CP932
    text_jp = "日本語"
    csv_content = f"Col1\n{text_jp}".encode("cp932")

    zip_bytes = create_zip_with_csvs({"demo.csv": csv_content})

    def mock_with_table_name(item: dict[str, Any], table_name: str) -> dict[str, Any]:
        item["__mock_table_name"] = table_name
        return item

    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:
        with patch("coreason_etl_pmda.sources_jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_page = MagicMock()
            mock_page.content = html_content.encode("utf-8")

            mock_zip = MagicMock()
            mock_zip.content = zip_bytes

            mock_get.side_effect = [mock_page, mock_zip]

            data = list(jader_source())
            assert len(data) == 1
            assert data[0]["Col1"] == text_jp
            assert data[0]["__mock_table_name"] == "bronze_jader_demo"


def test_jader_source_ignore_other_files() -> None:
    """Test that non-matching CSVs or other files are ignored."""
    html_content = """<a href="data.zip">Data</a>"""

    zip_bytes = create_zip_with_csvs(
        {"random.csv": "A,B\n1,2", "image.png": b"png data", "demo_backup.txt": "not a csv"}
    )

    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:
        mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

        data = list(jader_source())
        assert len(data) == 0


def test_jader_source_multiple_zips() -> None:
    """Test handling multiple zip links."""
    html_content = """
    <a href="part1.zip">Part 1</a>
    <a href="part2.zip">Part 2</a>
    """

    zip1 = create_zip_with_csvs({"demo1.csv": "ID\n1"})
    zip2 = create_zip_with_csvs({"demo2.csv": "ID\n2"})

    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:

        def side_effect(url: str, *args: Any, **kwargs: Any) -> MagicMock:
            if "part1" in url:
                return MagicMock(content=zip1)
            if "part2" in url:
                return MagicMock(content=zip2)
            return MagicMock(content=html_content.encode("utf-8"))

        mock_get.side_effect = side_effect

        data = list(jader_source())
        assert len(data) == 2
        ids = sorted([d["ID"] for d in data])
        assert ids == ["1", "2"]


def test_jader_source_broken_zip() -> None:
    """Test robust handling of a broken zip file."""
    html_content = """<a href="broken.zip">Broken</a>"""

    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:
        mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=b"not a zip file")]

        # The source logs error but continues.
        with patch("coreason_etl_pmda.sources_jader.logger.exception") as mock_log:
            data = list(jader_source())
            assert len(data) == 0
            mock_log.assert_called()


def test_jader_source_http_error() -> None:
    """Test HTTP error on main page."""
    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404")
        mock_get.return_value = mock_resp

        with pytest.raises(ResourceExtractionError):
            list(jader_source())


def test_jader_source_decode_failure() -> None:
    """Test handling of CSV decode failure within valid zip."""
    html_content = """<a href="data.zip">Data</a>"""

    zip_bytes = create_zip_with_csvs(
        {
            "demo.csv": b"\xff\xff\xff"  # random junk
        }
    )

    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:
        mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

        # Mock pl.read_csv to raise Exception for all calls
        with patch("coreason_etl_pmda.sources_jader.pl.read_csv", side_effect=Exception("Decode failed")):
            with patch("coreason_etl_pmda.sources_jader.logger.error") as mock_log:
                data = list(jader_source())
                assert len(data) == 0
                mock_log.assert_called_with(
                    "Failed to decode demo.csv in https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/data.zip"
                )
