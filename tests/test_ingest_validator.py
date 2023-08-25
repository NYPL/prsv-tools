import pytest
import requests
from pathlib import Path
import xml.etree.ElementTree as ET
import json

import prsv_tools.utility.api as prsvapi
import prsv_tools.ingest.ingest_validator as ingest_validator


# set up

content_endpoint = "https://nypl.preservica.com/api/content"
entity_endpoint = "https://nypl.preservica.com/api/entity"
test_digarch_uuid = "c0b9b47a-5552-4277-874e-092b3cc53af6"
token = prsvapi.get_token("test-ingest")
collectionid = "M848"

# unit tests
def test_content_searchwithin_so_endpoint():
    response = ingest_validator.search_within_DigArch(token, collectionid, test_digarch_uuid)

    assert response.status_code == 200








"""
1. response code should be good (200)
2. data structure should be as expected
"""