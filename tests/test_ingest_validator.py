import pytest
import requests
from pathlib import Path
import xml.etree.ElementTree as ET
import json

import prsv_tools.utility.api as prsvapi
import prsv_tools.ingest.ingest_validator as ingest_validator


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

# unit tests
def test_content_searchwithin_so_endpoint():
    # first test that the status code is good (200)
    response = ingest_validator.search_within_DigArch(token, collectionid, test_digarch_uuid)
    assert response.status_code == 200

    # second test that the response text has the conceived structure,
    # which can be parsed correctly into a not empty list
    uuid_ls = ingest_validator.parse_structural_object_uuid(response)
    assert type(uuid_ls) == list
    assert len(uuid_ls) > 0

def test_get_so_metadata():
    er_dict = ingest_validator.get_so_metadata(test_er_uuid, token, namespaces)
    # get_so_metadata may need to be separate into different sections
    # currently it is doing many things
    # first test get_api_result with the structural-objects/uuid response
    # then test data structure
    pass








"""
1. response code should be good (200)
2. data structure should be as expected
"""