# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from coreason_etl_pmda.sources.review_reports import review_reports_source


@pytest.fixture  # type: ignore[misc]
def mock_fetch_url() -> Iterator[MagicMock]:
    with patch("coreason_etl_pmda.sources.common.fetch_url") as mock_common:
        with patch("coreason_etl_pmda.sources.review_reports.fetch_url", new=mock_common):
            yield mock_common


def test_review_reports_complex_ambiguous_headers(mock_fetch_url: MagicMock) -> None:
    """
    Test extraction when multiple columns contain "承認番号".
    The system should prefer the exact match "承認番号" over "旧承認番号" or others.
    """
    html = """
    <html>
        <body>
            <table>
                <tr>
                    <th>旧承認番号</th>
                    <th>承認番号</th>
                    <th>販売名</th>
                    <th>審査報告書</th>
                </tr>
                <tr>
                    <td>Old-999</td>
                    <td>New-123</td>
                    <td>Drug A</td>
                    <td><a href="report.pdf">Link</a></td>
                </tr>
            </table>
        </body>
    </html>
    """
    # Note: common.py strips whitespace from headers.

    mock_main = MagicMock()
    mock_main.content = html.encode("utf-8")
    mock_main.encoding = "utf-8"
    mock_main.original_encoding = "utf-8"

    mock_pdf = MagicMock()
    mock_pdf.content = b"PDF Content"

    def get_side_effect(url: str, *args: list[object], **kwargs: dict[str, object]) -> MagicMock:
        if "pdf" in url:
            return mock_pdf
        return mock_main

    mock_fetch_url.side_effect = get_side_effect

    source = review_reports_source()
    data = list(source)

    assert len(data) == 1
    item = data[0]
    # We expect "New-123", not "Old-999"
    assert item["raw_payload"]["approval_id"] == "New-123"


def test_review_reports_complex_partial_match_fallback(mock_fetch_url: MagicMock) -> None:
    """
    Test extraction when EXACT "承認番号" is missing, but a likely candidate exists
    (e.g., "承認番号（主）").

    Strategy:
    1. Look for exact "承認番号".
    2. If not found, look for keys STARTING with "承認番号".
    3. Avoid keys like "旧承認番号".
    """
    html = """
    <html>
        <body>
            <table>
                <tr>
                    <th>承認番号(変)</th>
                    <th>販売名</th>
                    <th>審査報告書</th>
                </tr>
                <tr>
                    <td>Var-456</td>
                    <td>Drug B</td>
                    <td><a href="report.pdf">Link</a></td>
                </tr>
            </table>
        </body>
    </html>
    """

    mock_main = MagicMock()
    mock_main.content = html.encode("utf-8")
    mock_main.encoding = "utf-8"
    mock_main.original_encoding = "utf-8"

    mock_pdf = MagicMock()
    mock_pdf.content = b"PDF"

    mock_fetch_url.side_effect = lambda u, *a, **k: mock_pdf if "pdf" in u else mock_main

    source = review_reports_source()
    data = list(source)

    assert len(data) == 1
    item = data[0]
    # We want this to pass (capture it) IF it's the best candidate.
    assert item["raw_payload"]["approval_id"] == "Var-456"


def test_review_reports_complex_empty_id_cell(mock_fetch_url: MagicMock) -> None:
    """Test when approval_id cell is empty."""
    html = """
    <html>
        <body>
            <table>
                <tr>
                    <th>承認番号</th>
                    <th>販売名</th>
                    <th>審査報告書</th>
                </tr>
                <tr>
                    <td></td>
                    <td>Drug C</td>
                    <td><a href="report.pdf">Link</a></td>
                </tr>
            </table>
        </body>
    </html>
    """
    mock_main = MagicMock()
    mock_main.content = html.encode("utf-8")
    mock_main.encoding = "utf-8"
    mock_main.original_encoding = "utf-8"
    mock_pdf = MagicMock()
    mock_pdf.content = b"PDF"
    mock_fetch_url.side_effect = lambda u, *a, **k: mock_pdf if "pdf" in u else mock_main

    source = review_reports_source()
    data = list(source)

    assert len(data) == 1
    assert data[0]["raw_payload"]["approval_id"] == ""


def test_review_reports_complex_no_id_column(mock_fetch_url: MagicMock) -> None:
    """Test when approval_id column is completely missing."""
    html = """
    <html>
        <body>
            <table>
                <tr>
                    <th>販売名</th>
                    <th>審査報告書</th>
                </tr>
                <tr>
                    <td>Drug D</td>
                    <td><a href="report.pdf">Link</a></td>
                </tr>
            </table>
        </body>
    </html>
    """
    mock_main = MagicMock()
    mock_main.content = html.encode("utf-8")
    mock_main.encoding = "utf-8"
    mock_main.original_encoding = "utf-8"
    mock_pdf = MagicMock()
    mock_pdf.content = b"PDF"
    mock_fetch_url.side_effect = lambda u, *a, **k: mock_pdf if "pdf" in u else mock_main

    source = review_reports_source()
    data = list(source)

    assert len(data) == 1
    assert data[0]["raw_payload"]["approval_id"] == ""
