from pathlib import Path

import pandas as pd
import pytest
import responses

from src import ingestion


def test_record_array_requires_objects():
    assert ingestion.record_array({"data": [{"id": 1}]}, ("data",)) == [{"id": 1}]
    with pytest.raises(TypeError, match="every data item"):
        ingestion.record_array({"data": [1]}, ("data",))


@responses.activate
def test_request_json_returns_exact_bytes():
    responses.add(
        responses.GET,
        "https://example.test/source",
        body=b'{"records":[1]}',
        status=200,
        content_type="application/json",
    )
    session = ingestion.retry_session("test", retries=0)
    payload, raw = ingestion.request_json(
        session,
        "GET",
        "https://example.test/source",
    )
    assert payload == {"records": [1]}
    assert raw == b'{"records":[1]}'


def test_excel_contract_detects_header(tmp_path: Path):
    workbook = tmp_path / "psgc.xlsx"
    frame = pd.DataFrame(
        [
            ["Publication note", "", ""],
            ["10-digit PSGC", "Name", "Geographic Level"],
            ["1300000000", "NCR", "Reg"],
        ]
    )
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, header=False, sheet_name="PSGC")

    contract = {
        "required": {
            "psgc_code": ["10-digit PSGC"],
            "name": ["Name"],
            "geographic_level": ["Geographic Level"],
        },
        "optional": {"old_name": ["Old Name"]},
        "scan_rows": 5,
    }
    result, metadata = ingestion.read_excel_contract(workbook, contract)
    assert metadata["sheet_name"] == "PSGC"
    assert metadata["header_row"] == 1
    assert result.loc[0, "psgc_code"] == "1300000000"
    assert result.loc[0, "old_name"] == ""
