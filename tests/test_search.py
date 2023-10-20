import json

import pytest

import prsv_tools.utility.search as prsvsearch


@pytest.fixture
def json_response():
    with open("tests/fixtures/response.json") as f:
        response = json.load(f)

    return response


COLL_SEARCH = [
    ["M1234", "all", "", 2],
    ["M1234", "", "all", 1],
    ["M1234", "all", "all", 3],
]


@pytest.mark.parametrize("coll, er, ami, count", COLL_SEARCH)
def test_search_for_collection(
    json_response, mocker, coll: str, er: str, ami: str, count: int
):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        "prsv_tools.utility.search.get_response",
        return_value=json_response,
    )
    results = prsvsearch.search(coll_id=coll, er_id=er, ami_id=ami)

    assert len(results) == count


def filter_response(original: dict, object_index):
    filtered = original.copy()
    filtered["value"]["totalHits"] = 1
    filtered["value"]["objectIds"] = [filtered["value"]["objectIds"][object_index]]
    filtered["value"]["metadata"] = [filtered["value"]["metadata"][object_index]]
    return filtered


ITEM_SEARCH = [
    ["M1234", "ER_2", "", 0, 1],
    ["", "", "123456", 2, 1],
]


@pytest.mark.parametrize("coll, er, ami, index, count", ITEM_SEARCH)
def test_search_for_item(
    json_response: dict, mocker, coll: str, er: str, ami: str, index: int, count: int
):
    filtered_response = filter_response(json_response, index)
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        "prsv_tools.utility.search.get_response",
        return_value=filtered_response,
    )
    results = prsvsearch.search(coll_id=coll, er_id=er, ami_id=ami)

    assert len(results) == count


def test_search_no_results(json_response, mocker):
    json_response["value"]["totalHits"] = 0
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        "prsv_tools.utility.search.get_response",
        return_value=json_response,
    )

    with pytest.raises(ValueError) as exc_info:
        prsvsearch.search(coll_id="xxx", er_id="xxx", ami_id="xxx")

    assert "expected one result, got none" in exc_info.value.args[0]
