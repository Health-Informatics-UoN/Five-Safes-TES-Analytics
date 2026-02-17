from polars import DataFrame
import pytest
import parse_bunny
import json

@pytest.fixture
def bunny_example() -> parse_bunny.BunnyTSVOutput:
    with open("tests/test-data/100concepts.json", "r") as f:
        bunny_json = json.load(f)
        return parse_bunny.BunnyTSVOutput.model_validate(bunny_json)

def test_bunny_tsv_fields(bunny_example):
    assert bunny_example.uuid == "123"
    assert bunny_example.status == "ok"
    assert bunny_example.collection_id == "test"
    assert bunny_example.message == ""
    assert bunny_example.protocolVersion == "v2"

def test_bunny_query_result_fields(bunny_example):
    assert bunny_example.queryResult.count == 374
    assert bunny_example.queryResult.datasetCount == 1

def test_bunny_table_parser():
    table = parse_bunny.parse_bunny("tests/test-data/100concepts.json")
    assert isinstance(table, DataFrame)
    assert len(table) == 374
    assert table.columns == [
            "BIOBANK",
            "CODE",
            "COUNT",
            "DESCRIPTION",
            "MIN",
            "Q1",
            "MEDIAN",
            "MEAN",
            "Q3",
            "MAX",
            "ALTERNATIVES",
            "DATASET",
            "OMOP",
            "OMOP_DESCR",
            "CATEGORY"
            ]
