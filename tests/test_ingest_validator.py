import pytest
from pathlib import Path
import xml.etree.ElementTree as ET
import json

import prsv_tools.ingest.ingest_validator as ingest_validator

def test_search_within_DigArch(accesstoken, collectionid, parentuuid):
    """test that search_within_DigArch returns the 200 status code """
    response = ingest_validator.search_within_DigArch(accesstoken, collectionid, parentuuid)
