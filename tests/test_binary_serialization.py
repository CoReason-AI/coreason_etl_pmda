# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pmda

import base64
import json
from typing import Any, Iterator

import dlt
import pytest


@dlt.resource(name="test_binary")
def binary_source() -> Iterator[dict[str, Any]]:
    yield {"id": 1, "raw_payload": {"content": b"some binary content"}}


def test_dlt_json_serialization_failure_with_bytes() -> None:
    """
    Verifies that dlt (and generic JSON serializers) fail with bytes in payload
    if not handled by the destination specific logic.
    """
    # DltResource needs to be iterated
    data = next(iter(binary_source()))

    # This should raise TypeError: Object of type bytes is not JSON serializable
    with pytest.raises(TypeError, match="bytes is not JSON serializable"):
        json.dumps(data)


def test_dlt_json_serialization_success_with_base64() -> None:
    """
    Verifies that Base64 encoded content is JSON serializable.
    """
    content = b"some binary content"
    b64_content = base64.b64encode(content).decode("utf-8")

    data = {"id": 1, "raw_payload": {"content": b64_content}}

    # Should succeed
    json_str = json.dumps(data)
    assert b64_content in json_str
