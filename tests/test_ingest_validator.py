import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import requests
from schema import And, Optional, Or, Regex, Schema, SchemaError, Use

import prsv_tools.ingest.ingest_validator as ingest_validator
import prsv_tools.utility.api as prsvapi

# set up

# content_endpoint = "https://nypl.preservica.com/api/content"
# entity_endpoint = "https://nypl.preservica.com/api/entity"
test_digarch_uuid = "c0b9b47a-5552-4277-874e-092b3cc53af6"
token = prsvapi.get_token("test-ingest")
collectionid = "M1126"
test_er_uuid = "ae7a1ea1-9a75-4348-807a-9923b1f22ad0"
version = prsvapi.find_apiversion(token)
namespaces = {
    "xip_ns": f"{{http://preservica.com/XIP/v{version}}}",
    "entity_ns": f"{{http://preservica.com/EntityAPI/v{version}}}",
    "spec_ns": f"{{http://nypl.org/prsv_schemas/specCollection}}",
}

query = {
    "q": "",
    "fields": [{"name": "spec.specCollectionID", "values": [collectionid]}],
}
q = json.dumps(query)

testendpoints = [
    f"https://nypl.preservica.com/api/content/search-within?q={q}&parenthierarchy={test_digarch_uuid}&start=0&max=-1&metadata=''",
    f"https://nypl.preservica.com/api/entity/structural-objects/{test_er_uuid}",
    f"https://nypl.preservica.com/api/entity/structural-objects/{test_er_uuid}/identifiers",
    f"https://nypl.preservica.com/api/entity/structural-objects/{test_er_uuid}/metadata/4e2b6d26-be94-4188-a968-29a3458166c4",
    f"https://nypl.preservica.com/api/entity/structural-objects/{test_er_uuid}/children",
]


@pytest.mark.parametrize("url", testendpoints)
def test_used_endpoints_are_valid(url):
    res = ingest_validator.get_api_results(token, url)
    assert res.status_code == 200


# unit tests
def test_content_searchwithin_so_endpoint():
    # test that the response text has the conceived structure,
    # which can be parsed correctly into a not empty list
    response = ingest_validator.search_within_DigArch(
        token, collectionid, test_digarch_uuid
    )

    uuid_ls = ingest_validator.parse_structural_object_uuid(response)
    assert type(uuid_ls) == list
    assert len(uuid_ls) > 0


def test_get_so_metadata():
    """test that get_so_metadata function returns a dictionary with title (str),
    secutiry tag (str), type (url endpoint), metadata fragment (url endpoint)
    and children (url endpoint)"""

    expected_schema = Schema(
        {
            "title": Regex(r"M\d+_(ER|DI|EM)_\d+"),
            "sectag": Or("open", "preservation"),
            "id_url": Regex(
                r"^https://nypl.preservica.com/api/entity/structural-objects/.{36}/identifiers$"
            ),
            "metadata_url": Regex(
                r"^https://nypl.preservica.com/api/entity/structural-objects/.{36}/metadata/.{36}$"
            ),
            "children_url": Regex(
                r"^https://nypl.preservica.com/api/entity/structural-objects/.{36}/children$"
            ),
        }
    )

    er_dict = ingest_validator.get_so_metadata(test_er_uuid, token, namespaces)

    assert expected_schema.is_valid(er_dict) == True


"""
1. response code should be good (200)
2. data structure should be as expected
"""
