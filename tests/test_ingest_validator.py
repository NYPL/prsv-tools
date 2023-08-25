import pytest
import requests
from pathlib import Path
import xml.etree.ElementTree as ET
import json

import prsv_tools.utility.api as prsvapi
import prsv_tools.ingest.ingest_validator as ingest_validator


# set up

entity_endpoint = "https://nypl.preservica.com/api/entity"
test_digarch_uuid = "c0b9b47a-5552-4277-874e-092b3cc53af6"
token = prsvapi.get_token("test-ingest")

# unit tests
def test_entity_searchwithin_so_endpoint():
    url = entity_endpoint + "/structural-objects/" + test_digarch_uuid
    response = ingest_validator.get_api_results(token, url)

    assert response.status_code == 200






"""
1. response code should be good (200)
2. data structure should be as expected
"""