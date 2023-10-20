import json

import pytest

import prsv_tools.utility.search as prsvsearch


@pytest.fixture
def json_response():
    with open("tests/fixtures/response.json") as f:
        response = json.load(f)

    return response


def test_search_for_collection(json_response, mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        "prsv_tools.utility.search.get_response",
        return_value=json_response,
    )
    results = prsvsearch.search(coll_id="M1234")
    print(results)
    assert len(results) == 4


## start search session
## return uuids based on results
## report if nothing is found

## should have option to return uuid or package?
