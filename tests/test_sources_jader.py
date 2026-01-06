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
from coreason_etl_pmda.sources.jader import jader_source
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


def mock_with_table_name(item: Any, table_name: str) -> Any:
    """Mock dlt.mark.with_table_name to inject a verification key into Arrow metadata?
    No, dlt markers work by wrapping.
    Since we are yielding an object, we can't easily modify it in place if it's C++ Arrow Table easily
    without reconstructing.
    However, for testing, we can check the call args if we mock it, OR we can return a wrapper.
    But the test consumes the generator.
    If we return a tuple (item, table_name) or wrapper object, we can verify.
    Let's return a wrapper class or just attach an attribute if possible (Arrow Tables are immutable-ish).
    Actually, we can wrap it in a simple class or dict for test purposes, provided the source yields it.
    """

    class TableWrapper:
        def __init__(self, table: Any, name: str):
            self.table = table
            self.table_name = name

    return TableWrapper(item, table_name)


def unwrap_arrow_data(data: list[Any]) -> list[dict[str, Any]]:
    """Helper to convert list of TableWrappers (holding Arrow tables) into list of dict rows."""
    rows = []
    for wrapper in data:
        # Check type
        # assert isinstance(wrapper, TableWrapper) # We can't import inner class easily
        table = wrapper.table
        tname = wrapper.table_name

        # Convert Arrow Table to pydict list
        # to_pylist() returns list of dicts
        batch_rows = table.to_pylist()
        for r in batch_rows:
            r["__mock_table_name"] = tname
            rows.append(r)
    return rows


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

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
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
            raw_data = list(resource)

            # We expect 3 chunks (one arrow table per file)
            assert len(raw_data) == 3

            # Unwrap to check rows
            data = unwrap_arrow_data(raw_data)
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

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_page = MagicMock()
            mock_page.content = html_content.encode("utf-8")

            mock_zip = MagicMock()
            mock_zip.content = zip_bytes

            mock_get.side_effect = [mock_page, mock_zip]

            raw_data = list(jader_source())
            data = unwrap_arrow_data(raw_data)

            assert len(data) == 1
            assert data[0]["Col1"] == text_jp
            assert data[0]["__mock_table_name"] == "bronze_jader_demo"


def test_jader_source_ignore_other_files() -> None:
    """Test that non-matching CSVs or other files are ignored."""
    html_content = """<a href="data.zip">Data</a>"""

    zip_bytes = create_zip_with_csvs(
        {"random.csv": "A,B\n1,2", "image.png": b"png data", "demo_backup.txt": "not a csv"}
    )

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

        raw_data = list(jader_source())
        assert len(raw_data) == 0


def test_jader_source_multiple_zips() -> None:
    """Test handling multiple zip links."""
    html_content = """
    <a href="part1.zip">Part 1</a>
    <a href="part2.zip">Part 2</a>
    """

    zip1 = create_zip_with_csvs({"demo1.csv": "ID\n1"})
    zip2 = create_zip_with_csvs({"demo2.csv": "ID\n2"})

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:

        def side_effect(url: str, *args: Any, **kwargs: Any) -> MagicMock:
            if "part1" in url:
                return MagicMock(content=zip1)
            if "part2" in url:
                return MagicMock(content=zip2)
            return MagicMock(content=html_content.encode("utf-8"))

        mock_get.side_effect = side_effect

        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            raw_data = list(jader_source())
            data = unwrap_arrow_data(raw_data)

            assert len(data) == 2
            ids = sorted([d["ID"] for d in data])
            assert ids == ["1", "2"]


def test_jader_source_broken_zip() -> None:
    """Test robust handling of a broken zip file."""
    html_content = """<a href="broken.zip">Broken</a>"""

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=b"not a zip file")]

        # The source logs error but continues.
        with patch("coreason_etl_pmda.sources.jader.logger.exception") as mock_log:
            raw_data = list(jader_source())
            assert len(raw_data) == 0
            mock_log.assert_called()


def test_jader_source_http_error() -> None:
    """Test HTTP error on main page."""
    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
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

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

        # Mock pl.read_csv to raise Exception for all calls
        with patch("coreason_etl_pmda.sources.jader.pl.read_csv", side_effect=Exception("Decode failed")):
            with patch("coreason_etl_pmda.sources.jader.logger.error") as mock_log:
                raw_data = list(jader_source())
                assert len(raw_data) == 0
                mock_log.assert_called_with(
                    "Failed to decode demo.csv in https://www.pmda.go.jp/safety/info-services/drugs/adr-info/suspected-adr/data.zip"
                )


def test_jader_source_filename_case_sensitivity() -> None:
    """Test that filenames are matched case-insensitively."""
    html_content = """<a href="data.zip">Data</a>"""

    zip_bytes = create_zip_with_csvs({"DEMO.CSV": "ID\n1", "Drug.csv": "ID\n1", "REAC.Csv": "ID\n1"})

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

            raw_data = list(jader_source())
            data = unwrap_arrow_data(raw_data)

            assert len(data) == 3
            files = {d["_source_file"] for d in data}
            assert files == {"DEMO.CSV", "Drug.csv", "REAC.Csv"}


def test_jader_source_empty_file() -> None:
    """Test handling of 0-byte or empty CSV files."""
    html_content = """<a href="data.zip">Data</a>"""

    zip_bytes = create_zip_with_csvs({"demo_empty.csv": b"", "demo_valid.csv": "ID\n1"})

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

            # Polars read_csv might raise NoDataError for empty file, or return empty DF.
            # If it raises Exception, our loop catches it and logs error.
            # We want to ensure valid files still process.

            with patch("coreason_etl_pmda.sources.jader.logger.error") as mock_log:
                raw_data = list(jader_source())
                data = unwrap_arrow_data(raw_data)

                # Should get data from valid file
                assert len(data) == 1
                assert data[0]["_source_file"] == "demo_valid.csv"

                # Should log error for empty file
                mock_log.assert_called()


def test_jader_source_mixed_encodings() -> None:
    """Test zip containing mixed encoding files."""
    html_content = """<a href="data.zip">Data</a>"""

    # Shift-JIS file
    sjis_content = f"Col\n{'日本語'}".encode("shift_jis")
    # UTF-8 file
    utf8_content = f"Col\n{'English'}"

    zip_bytes = create_zip_with_csvs({"demo_sjis.csv": sjis_content, "demo_utf8.csv": utf8_content})

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

            raw_data = list(jader_source())
            data = unwrap_arrow_data(raw_data)

            assert len(data) == 2

            row_sjis = next(d for d in data if d["_source_file"] == "demo_sjis.csv")
            assert row_sjis["Col"] == "日本語"

            row_utf8 = next(d for d in data if d["_source_file"] == "demo_utf8.csv")
            assert row_utf8["Col"] == "English"


def test_jader_source_duplicate_tables() -> None:
    """Test multiple files mapping to same table."""
    html_content = """<a href="data.zip">Data</a>"""

    zip_bytes = create_zip_with_csvs({"demo_part1.csv": "ID\n1", "demo_part2.csv": "ID\n2"})

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

            raw_data = list(jader_source())
            data = unwrap_arrow_data(raw_data)

            assert len(data) == 2

            # Both should map to bronze_jader_demo
            assert all(d["__mock_table_name"] == "bronze_jader_demo" for d in data)

            ids = sorted([d["ID"] for d in data])
            assert ids == ["1", "2"]
