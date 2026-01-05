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

from coreason_etl_pmda.sources_approvals import approvals_source


def test_full_width_whitespace_headers() -> None:
    """Test headers with full-width spaces (Zenkaku space) are normalized."""
    # 販\u3000売\u3000名 -> 販売名
    # 承\u3000認\u3000番\u3000号 -> 承認番号
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th>販　売　名</th>
                    <th>承　認　番　号</th>
                    <th>承認年月日</th>
                </tr>
                <tr>
                    <td>DrugA</td>
                    <td>123</td>
                    <td>R2.1.1</td>
                </tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        payload = data[0]["raw_payload"]

        # Keys should be normalized (whitespace stripped)
        # The code uses `re.sub(r"\s+", "", text)`.
        # \s matches unicode whitespace including \u3000.
        assert "販売名" in payload
        assert "承認番号" in payload
        assert payload["販売名"] == "DrugA"
        assert payload["承認番号"] == "123"


def test_multiple_links_in_report_cell() -> None:
    """
    Verify behavior when multiple links exist in the Report cell (e.g., Part 1, Part 2).
    Current logic takes the FIRST link found.
    """
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th>販売名</th>
                    <th>審査報告書</th>
                    <th>承認年月日</th>
                    <th>一般的名称</th>
                </tr>
                <tr>
                    <td>DrugB</td>
                    <td>
                        <a href="part1.pdf">Part 1</a>
                        <br>
                        <a href="part2.pdf">Part 2</a>
                    </td>
                    <td>R2.1.1</td>
                    <td>GenB</td>
                </tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source(url="http://base.com/"))
        assert len(data) == 1
        payload = data[0]["raw_payload"]

        # Expecting the first link
        assert payload["review_report_url"] == "http://base.com/part1.pdf"


def test_empty_critical_cells() -> None:
    """
    Verify extraction when 'Approval Number' or 'Brand Name' is empty.
    Logic: `has_brand = any("販売名" in k for k in record.keys())`
    Wait, `headers` determine keys. If header exists, key exists.
    If cell is empty, value is empty string.
    So "has_brand" check passes if header exists.
    """
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th>販売名</th>
                    <th>承認番号</th>
                    <th>承認年月日</th>
                    <th>一般的名称</th>
                </tr>
                <!-- Row with Empty Brand Name (Should be included? or filtered?) -->
                <!-- The code checks: has_brand = any("販売名" in k for k in record.keys()) -->
                <!-- It does NOT check if value is truthy. -->
                <tr>
                    <td></td>
                    <td>999</td>
                    <td>R2.1.1</td>
                    <td>GenEmpty</td>
                </tr>
                <!-- Row with Empty Approval Number -->
                <tr>
                    <td>DrugC</td>
                    <td></td>
                    <td>R2.1.1</td>
                    <td>GenC</td>
                </tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 2

        # Row 1: Empty Brand Name
        row1 = data[0]["raw_payload"]
        assert row1["販売名"] == ""
        assert row1["承認番号"] == "999"

        # Row 2: Empty Approval Number
        row2 = data[1]["raw_payload"]
        assert row2["販売名"] == "DrugC"
        assert row2["承認番号"] == ""


def test_duplicate_headers() -> None:
    """
    Verify behavior with duplicate headers.
    Python dicts overwrite duplicate keys.
    Last one wins?
    """
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th>販売名</th>
                    <th>備考</th>
                    <th>承認年月日</th>
                    <th>備考</th> <!-- Duplicate -->
                    <th>一般的名称</th>
                </tr>
                <tr>
                    <td>DrugD</td>
                    <td>Note1</td>
                    <td>R2.1.1</td>
                    <td>Note2</td>
                    <td>GenD</td>
                </tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        payload = data[0]["raw_payload"]

        # Verify "備考" value.
        # Loop: `for idx, header in enumerate(headers): record[header] = cell_text`
        # 2nd "備考" (idx 3) will overwrite 1st "備考" (idx 1).
        assert payload["備考"] == "Note2"


def test_interleaved_unexpected_columns() -> None:
    """Test extraction when unknown columns are present."""
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <th>販売名</th>
                    <th>謎の列</th> <!-- Mystery Column -->
                    <th>一般的名称</th>
                    <th>承認年月日</th>
                </tr>
                <tr>
                    <td>DrugE</td>
                    <td>MysteryValue</td>
                    <td>GenE</td>
                    <td>R2.1.1</td>
                </tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        assert len(data) == 1
        payload = data[0]["raw_payload"]

        assert payload["販売名"] == "DrugE"
        assert payload["謎の列"] == "MysteryValue"
        assert payload["一般的名称"] == "GenE"


def test_missing_critical_columns() -> None:
    """
    Test tables that match keyword heuristic but miss 'Brand Name' column.
    The code: `matches = sum(1 for k in keywords if any(k in h for h in headers))`
    Keywords: ["販売名", "一般的名称", "承認年月日"]
    Heuristic: matches >= 2.

    Case: Table has '一般的名称' and '承認年月日', but NOT '販売名'.
    The loop continues, `has_brand` check fails (if it looks for '販売名' key).
    """
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <!-- Missing Brand Name -->
                    <th>一般的名称</th>
                    <th>承認年月日</th>
                    <th>申請者氏名</th>
                </tr>
                <tr>
                    <td>GenF</td>
                    <td>R2.1.1</td>
                    <td>ApplicantF</td>
                </tr>
            </table>
        </body>
    </html>
    """
    with patch("coreason_etl_pmda.sources_approvals.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.content = html_content.encode("utf-8")
        mock_resp.encoding = "utf-8"
        mock_get.return_value = mock_resp

        data = list(approvals_source())
        # Matches heuristic (2 keywords: 一般的名称, 承認年月日).
        # But `has_brand = any("販売名" in k for k in record.keys())` should fail.
        # So it yields nothing.
        assert len(data) == 0
