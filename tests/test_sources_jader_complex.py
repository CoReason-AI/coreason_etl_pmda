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

import pyarrow as pa
from coreason_etl_pmda.sources.jader import jader_source


# Reusing helper pattern
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


class TableWrapper:
    def __init__(self, table: Any, name: str):
        self.table = table
        self.table_name = name


def mock_with_table_name(item: Any, table_name: str) -> Any:
    return TableWrapper(item, table_name)


def unwrap_arrow_data(data: list[Any]) -> list[dict[str, Any]]:
    rows = []
    for wrapper in data:
        table = wrapper.table
        tname = wrapper.table_name
        batch_rows = table.to_pylist()
        for r in batch_rows:
            r["__mock_table_name"] = tname
            rows.append(r)
    return rows


def test_jader_source_nested_directories() -> None:
    """Test CSVs nested inside folders in the zip file."""
    html_content = """<a href="data.zip">Data</a>"""

    zip_bytes = create_zip_with_csvs({"folder/subfolder/demo_nested.csv": "ID\n1", "root_demo.csv": "ID\n2"})

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

            raw_data = list(jader_source())
            data = unwrap_arrow_data(raw_data)

            assert len(data) == 2

            # Check paths
            nested_row = next(d for d in data if d["ID"] == "1")
            assert nested_row["_source_file"] == "folder/subfolder/demo_nested.csv"
            assert nested_row["__mock_table_name"] == "bronze_jader_demo"

            root_row = next(d for d in data if d["ID"] == "2")
            assert root_row["_source_file"] == "root_demo.csv"


def test_jader_source_complex_csv_syntax() -> None:
    """Test parsing of CSVs with quoted strings and newlines."""
    html_content = """<a href="data.zip">Data</a>"""

    # CSV with quoted newline
    complex_csv = 'ID,Note\n1,"Line1\nLine2"\n2,"Simple"'

    zip_bytes = create_zip_with_csvs({"demo_complex.csv": complex_csv})

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

            raw_data = list(jader_source())
            data = unwrap_arrow_data(raw_data)

            assert len(data) == 2

            row1 = next(d for d in data if d["ID"] == "1")
            # Polars should preserve the newline inside the quoted string
            assert row1["Note"] == "Line1\nLine2"

            row2 = next(d for d in data if d["ID"] == "2")
            assert row2["Note"] == "Simple"


def test_jader_source_type_inference_safety() -> None:
    """Verify that columns looking like integers/dates are kept as strings (infer_schema_length=0)."""
    html_content = """<a href="data.zip">Data</a>"""

    # ID looks like int, Date looks like date, Mixed has int and string
    csv_content = "ID,Date,Mixed\n1,2020-01-01,100\n2,2020-01-02,Text"

    zip_bytes = create_zip_with_csvs({"demo_types.csv": csv_content})

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

            raw_data = list(jader_source())

            # Check Arrow Schema directly before unwrapping
            assert len(raw_data) == 1
            arrow_table = raw_data[0].table
            schema = arrow_table.schema

            # All columns should be string (Utf8 or LargeUtf8)
            # Polars usually uses LargeUtf8 for strings
            # ID might be inferred as string because we passed infer_schema_length=0

            for field in schema:
                if field.name in ["ID", "Date", "Mixed"]:
                    # PyArrow type check
                    assert pa.types.is_string(field.type) or pa.types.is_large_string(
                        field.type
                    ), f"Column {field.name} should be string type, got {field.type}"

            data = unwrap_arrow_data(raw_data)
            assert data[0]["ID"] == "1"  # String
            assert data[0]["Mixed"] == "100"  # String


def test_jader_source_schema_evolution_simulation() -> None:
    """Test handling of multiple files with different schemas (simulating schema evolution)."""
    html_content = """<a href="data.zip">Data</a>"""

    # Two demo files with different columns
    demo_v1 = "ID,Name\n1,A"
    demo_v2 = "ID,Name,Age\n2,B,30"

    zip_bytes = create_zip_with_csvs({"demo_v1.csv": demo_v1, "demo_v2.csv": demo_v2})

    with patch("coreason_etl_pmda.sources.jader.fetch_url") as mock_get:
        with patch("coreason_etl_pmda.sources.jader.dlt.mark.with_table_name", side_effect=mock_with_table_name):
            mock_get.side_effect = [MagicMock(content=html_content.encode("utf-8")), MagicMock(content=zip_bytes)]

            raw_data = list(jader_source())

            # We expect two separate arrow tables yielded
            assert len(raw_data) == 2

            # Verify they are separate and have different schemas
            table1 = next(r.table for r in raw_data if "demo_v1.csv" in str(r.table.column("_source_file")[0]))
            table2 = next(r.table for r in raw_data if "demo_v2.csv" in str(r.table.column("_source_file")[0]))

            assert "Age" not in table1.column_names
            assert "Age" in table2.column_names

            # Unwrapped data should contain all
            data = unwrap_arrow_data(raw_data)
            assert len(data) == 2
