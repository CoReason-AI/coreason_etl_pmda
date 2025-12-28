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
from unittest.mock import MagicMock, patch

from coreason_etl_pmda.sources_jader import jader_source


def test_jader_source() -> None:
    # Mock HTML with Zip link
    html_content = """
    <html>
        <body>
            <a href="jader_data.zip">Download JADER</a>
        </body>
    </html>
    """

    # Create a mock Zip file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as z:
        z.writestr("demo.csv", "id,age\n1,20")
        z.writestr("drug.csv", "id,drug_name\n1,Aspirin")
        z.writestr("reac.csv", "id,reaction\n1,Headache")
        z.writestr("other.txt", "ignore me")

    zip_content = zip_buffer.getvalue()

    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:

        def side_effect(url: str) -> MagicMock:
            resp = MagicMock()
            resp.raise_for_status.return_value = None
            if "html" in url:
                resp.content = html_content.encode("utf-8")
            elif url.endswith(".zip"):
                resp.content = zip_content
            else:
                resp.status_code = 404
            return resp

        mock_get.side_effect = side_effect

        resource = jader_source(url="http://example.com/jader.html")

        # Collect items
        items = list(resource)

        # We expect items for demo, drug, reac
        # 1 row each
        assert len(items) == 3

        # Verify table names
        # dlt marks items with specific metadata.
        # But `list(resource)` yields the data items. The table name is in the metadata attached.
        # However, `dlt.mark.with_table_name` wraps the item.
        # Actually, `dlt.mark.with_table_name` modifies the item if it's a dict?
        # Or returns a wrapper?
        # It returns the item itself but with metadata if it's a dict.

        # Let's check table names via logic or just content
        # We can't easily inspect the dlt meta table name on the dict directly in unit test
        # without dlt internals, but we can check the content.

        demo_rows = [i for i in items if i.get("age") == 20]
        drug_rows = [i for i in items if i.get("drug_name") == "Aspirin"]
        reac_rows = [i for i in items if i.get("reaction") == "Headache"]

        assert len(demo_rows) == 1
        assert len(drug_rows) == 1
        assert len(reac_rows) == 1

        assert demo_rows[0]["_source_file"] == "demo.csv"


def test_jader_source_encoding() -> None:
    # Test encoding fallback (Shift-JIS)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as z:
        # CP932 content
        content = "id,name\n1,日本語".encode("cp932")
        z.writestr("demo.csv", content)

    zip_content = zip_buffer.getvalue()

    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:
        mock_resp_html = MagicMock()
        mock_resp_html.content = b'<html><a href="data.zip">Link</a></html>'

        mock_resp_zip = MagicMock()
        mock_resp_zip.content = zip_content

        mock_get.side_effect = [mock_resp_html, mock_resp_zip]

        resource = jader_source()
        items = list(resource)

        assert len(items) == 1
        assert items[0]["name"] == "日本語"


def test_jader_source_error() -> None:
    # Zip download fails
    with patch("coreason_etl_pmda.sources_jader.requests.get") as mock_get:
        mock_resp_html = MagicMock()
        mock_resp_html.content = b'<html><a href="data.zip">Link</a></html>'

        mock_resp_zip = MagicMock()
        mock_resp_zip.raise_for_status.side_effect = Exception("Fail")

        mock_get.side_effect = [mock_resp_html, mock_resp_zip]

        resource = jader_source()
        items = list(resource)

        assert len(items) == 0
