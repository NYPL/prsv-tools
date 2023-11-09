import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, replace
from pathlib import Path

import pytest
import requests
from schema import And, Optional, Or, Regex, Schema, SchemaError, Use

import prsv_tools.ingest.ingest_validator as ingest_validator
import prsv_tools.utility.api as prsvapi

# set up

test_digarch_uuid = "c0b9b47a-5552-4277-874e-092b3cc53af6"
token = prsvapi.get_token("test-ingest")
collectionid = "M1126"
test_er_uuid = "ae7a1ea1-9a75-4348-807a-9923b1f22ad0"
test_contents_uuid = "9885feb6-3340-4a02-8585-e0f75f55eb92"
test_metadata_uuid = "5df3566c-36f0-4721-9e5e-bc1c824b5910"
version = prsvapi.find_apiversion(token)
namespaces = {
    "xip_ns": f"{{http://preservica.com/XIP/v{version}}}",
    "entity_ns": f"{{http://preservica.com/EntityAPI/v{version}}}",
    "spec_ns": f"{{http://nypl.org/prsv_schemas/specCollection}}",
    "fa_ns": f"{{http://nypl.org/prsv_schemas/findingAid}}",
}

fields = [{"name": "spec.specCollectionID", "values": [collectionid]}]

query = {"q": "", "fields": fields}
q = json.dumps(query)

testendpoints = [
    f"https://nypl.preservica.com/api/content/search-within?q={q}&parenthierarchy={test_digarch_uuid}&start=0&max=-1&metadata=''",
    f"https://nypl.preservica.com/api/entity/structural-objects/{test_er_uuid}",
    f"https://nypl.preservica.com/api/entity/structural-objects/{test_er_uuid}/identifiers",
    f"https://nypl.preservica.com/api/entity/structural-objects/{test_er_uuid}/metadata/4e2b6d26-be94-4188-a968-29a3458166c4",
    f"https://nypl.preservica.com/api/entity/structural-objects/{test_er_uuid}/children",
]

uuid_pattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


@pytest.mark.parametrize("url", testendpoints)
def test_used_endpoints_are_valid(url):
    res = ingest_validator.get_api_results(token, url)
    assert res.status_code == 200


@dataclass
class prsv_Structural_Object:
    uuid: str
    title: str
    type: str
    securityTag: str
    soCategory: str
    mdFragments: dict | None
    children: dict | None


# unit tests
def test_content_searchwithin_so_endpoint():
    # test that the response text has the conceived structure,
    # which is a non-empty list consisting of UUID(s)
    response = ingest_validator.search_within_DigArch(token, fields, test_digarch_uuid)

    uuid_ls = ingest_validator.parse_structural_object_uuid(response)

    expected_schema = Schema(
        [Regex(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")]
    )

    assert expected_schema.is_valid(uuid_ls) == True


def test_mock_get_top_so():
    """test that get_so function returns the correct
    data class structure for the top level SO"""

    # mock value needs to be modified. This test potentially replaces test_get_top_so
    mock_call_api_value = prsv_Structural_Object(
        uuid="658e4d63-ccfa-41e8-83ab-4caaf3a1b061",
        title="M24468_ER_8",
        type="soCategory",
        securityTag="open",
        soCategory="ERContainer",
        mdFragments={"speccolID": "M24468"},
        children={
            "M24468_ER_8_contents": {
                "objType": "SO",
                "uuid": "84db17ec-acbc-4b06-8cb2-3ceac63eeb00",
            },
            "M24468_ER_8_metadata": {
                "objType": "SO",
                "uuid": "4b4acc77-8310-44e9-bac3-3b214968c797",
            },
        },
    )
    actual_api_value = ingest_validator.get_so(test_er_uuid, token, namespaces, "top")
    assert mock_call_api_value == actual_api_value


def test_get_top_so():
    """test that get_so function returns the correct
    data class structure for the top level SO"""

    top_so_dataclass = ingest_validator.get_so(test_er_uuid, token, namespaces, "top")
    assert top_so_dataclass.uuid == test_er_uuid
    assert re.fullmatch(r"M[0-9]+_(ER|DI|EM)_[0-9]+", top_so_dataclass.title)
    assert top_so_dataclass.type == "soCategory"
    assert top_so_dataclass.securityTag == "open"
    assert top_so_dataclass.soCategory in ["ERContainer", "DIContainer", "EMContainer"]
    assert isinstance(top_so_dataclass.mdFragments, dict)
    assert "speccolID" in top_so_dataclass.mdFragments
    assert re.fullmatch(r"M[0-9]+", top_so_dataclass.mdFragments["speccolID"])
    assert isinstance(top_so_dataclass.children, dict)
    children_schema = Schema(
        {
            Regex(r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents"): {
                "objType": "SO",
                "uuid": Regex(f"{uuid_pattern}"),
            },
            Regex(r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata"): {
                "objType": "SO",
                "uuid": Regex(f"{uuid_pattern}"),
            },
        }
    )
    assert children_schema.is_valid(top_so_dataclass.children) == True


def test_mock_get_contents_so():
    """test that get_so function returns the correct
    data class structure for the contents SO"""

    # mock value needs to be modified. This test potentially replaces test_get_contents_so
    mock_call_api_value = prsv_Structural_Object(
        uuid="84db17ec-acbc-4b06-8cb2-3ceac63eeb00",
        title="M24468_ER_8_contents",
        type="soCategory",
        securityTag="open",
        soCategory="ERContents",
        mdFragments={
            "erNumber": "ER_8",
            "faCollectionId": "M24468",
            "faComponentId": "M24468_ER_8",
        },
        children={
            "INVOICEpostage.xls": {
                "objType": "IO",
                "uuid": "f4d3eecf-d621-4f63-8c0f-e9a7d4717492",
            },
            "Tom Slaughter.doc": {
                "objType": "IO",
                "uuid": "813fc586-1044-4661-98f7-87d9904644de",
            },
            "authors fee.doc": {
                "objType": "IO",
                "uuid": "ec42db69-6e77-400c-8051-a60362628104",
            },
            "beth.doc": {
                "objType": "IO",
                "uuid": "1892f8cb-1f94-4830-a9ee-48ba7eb181d6",
            },
            "bio fax.doc": {
                "objType": "IO",
                "uuid": "a74558f9-6002-4575-92d8-c79a9554c3cd",
            },
            "budget memo.doc": {
                "objType": "IO",
                "uuid": "8305e1bd-90d6-41a1-9274-969ef4f7783f",
            },
            "budget.xls": {
                "objType": "IO",
                "uuid": "c278bb6d-e6b2-46f6-a7d3-7a198f969635",
            },
            "equity.xls": {
                "objType": "IO",
                "uuid": "dccd76ae-993e-47be-9415-98036d59a213",
            },
            "snakebit llc.xls": {
                "objType": "IO",
                "uuid": "64c479d4-3f5e-45be-a6d3-5e72398fda4a",
            },
            "snakebit program notes.doc": {
                "objType": "IO",
                "uuid": "a9830d4f-5bc6-411d-98db-f78e52beb4b3",
            },
            "snakebit program notes2.doc": {
                "objType": "IO",
                "uuid": "411a2a30-f6ec-4463-bf91-8ada46a11b7c",
            },
        },
    )
    actual_api_value = ingest_validator.get_so(
        test_er_uuid, token, namespaces, "contents"
    )

    assert mock_call_api_value == actual_api_value


def test_get_contents_so():
    """test that get_so function returns the correct
    data class structure for the contents SO"""

    contents_so_dataclass = ingest_validator.get_so(
        test_contents_uuid, token, namespaces, "contents"
    )
    assert re.fullmatch(uuid_pattern, contents_so_dataclass.uuid)
    assert re.fullmatch(
        r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents", contents_so_dataclass.title
    )
    assert contents_so_dataclass.type == "soCategory"
    assert contents_so_dataclass.securityTag == "open"
    assert contents_so_dataclass.soCategory in [
        "ERContents",
        "DIContents",
        "EMContents",
    ]
    assert isinstance(contents_so_dataclass.mdFragments, dict)
    assert "erNumber" in contents_so_dataclass.mdFragments
    assert re.fullmatch(
        f"(ER|DI|EM)_[0-9]+", contents_so_dataclass.mdFragments["erNumber"]
    )
    assert "faCollectionId" in contents_so_dataclass.mdFragments
    assert re.fullmatch(f"M[0-9]+", contents_so_dataclass.mdFragments["faCollectionId"])
    assert "faComponentId" in contents_so_dataclass.mdFragments
    assert re.fullmatch(
        f"M[0-9]+_(ER|DI|EM)_[0-9]+", contents_so_dataclass.mdFragments["faComponentId"]
    )
    assert isinstance(contents_so_dataclass.children, dict)
    child_schema = Schema(
        {Regex(r".+"): {"objType": Or("SO", "IO"), "uuid": Regex(f"{uuid_pattern}")}}
    )
    assert child_schema.is_valid(contents_so_dataclass.children)


def test_mock_get_contents_so():
    """test that get_so function returns the correct
    data class structure for the contents SO"""

    # mock value needs to be modified. This test potentially replaces test_get_metadata_so
    mock_call_api_value = prsv_Structural_Object(
        uuid="bf45162f-a0b2-418c-8b8f-1ef177e58a19",
        title="M1126_DI_1_metadata",
        type="soCategory",
        securityTag="preservation",
        soCategory="DIMetadata",
        mdFragments=None,
        children={
            "M1126-0046p001.JPG": {
                "objType": "IO",
                "uuid": "36288300-28f7-4edc-b4f3-f6272de64ac5",
            }
        },
    )
    actual_api_value = ingest_validator.get_so(
        test_er_uuid, token, namespaces, "metadata"
    )

    assert mock_call_api_value == actual_api_value


def test_get_so_metadata():
    """test that get_so function returns the correct
    data class structure for the metadata SO"""

    metadata_so_dataclass = ingest_validator.get_so(
        test_metadata_uuid, token, namespaces, "metadata"
    )
    assert re.fullmatch(uuid_pattern, metadata_so_dataclass.uuid)
    assert re.fullmatch(
        r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata", metadata_so_dataclass.title
    )
    assert metadata_so_dataclass.type == "soCategory"
    assert metadata_so_dataclass.securityTag == "preservation"
    assert metadata_so_dataclass.soCategory in [
        "ERMetadata",
        "DIMetadata",
        "EMMetadata",
    ]
    assert metadata_so_dataclass.mdFragments == None
    assert isinstance(metadata_so_dataclass.children, dict)
    child_schema = Schema(
        {Regex(r".+"): {"objType": Or("SO", "IO"), "uuid": Regex(f"{uuid_pattern}")}}
    )
    assert child_schema.is_valid(metadata_so_dataclass.children)

@pytest.fixture
def valid_prsv_top():
    prsv_top = prsv_Structural_Object(uuid='658e4d63-ccfa-41e8-83ab-4caaf3a1b061',
                       title='M24468_ER_8',
                       type='soCategory',
                       securityTag='open',
                       soCategory='ERContainer',
                       mdFragments={'speccolID': 'M24468'},
                       children={'M24468_ER_8_contents': {'objType': 'SO',
                                                          'uuid': '84db17ec-acbc-4b06-8cb2-3ceac63eeb00'},
                                 'M24468_ER_8_metadata': {'objType': 'SO',
                                                          'uuid': '4b4acc77-8310-44e9-bac3-3b214968c797'}})
    return prsv_top

@pytest.fixture
def valid_prsv_contents():
    prsv_contents = prsv_Structural_Object(uuid='84db17ec-acbc-4b06-8cb2-3ceac63eeb00',
                       title='M24468_ER_8_contents',
                       type='soCategory',
                       securityTag='open',
                       soCategory='ERContents',
                       mdFragments={'erNumber': 'ER_8',
                                    'faCollectionId': 'M24468',
                                    'faComponentId': 'M24468_ER_8'},
                       children={'INVOICEpostage.xls': {'objType': 'IO',
                                                        'uuid': 'f4d3eecf-d621-4f63-8c0f-e9a7d4717492'},
                                 'Tom Slaughter.doc': {'objType': 'IO',
                                                       'uuid': '813fc586-1044-4661-98f7-87d9904644de'}})
    return prsv_contents

@pytest.fixture
def valid_prsv_metadata():
    prsv_metadata = prsv_Structural_Object(uuid='bf45162f-a0b2-418c-8b8f-1ef177e58a19',
                       title='M1126_DI_1_metadata',
                       type='soCategory',
                       securityTag='preservation',
                       soCategory='DIMetadata',
                       mdFragments=None,
                       children={'M1126-0046p001.JPG': {'objType': 'IO',
                                                        'uuid': '36288300-28f7-4edc-b4f3-f6272de64ac5'}})
    return prsv_metadata

def test_validate_so_title(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that validate_so_title returns True when
    the title field matches the pattern"""

    assert ingest_validator.validate_so_title(valid_prsv_top, r"M[0-9]+_(ER|DI|EM)_[0-9]+")
    assert ingest_validator.validate_so_title(valid_prsv_contents, r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents")
    assert ingest_validator.validate_so_title(valid_prsv_metadata, r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata")

def test_invalid_so_title(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that validate_so_title returns False when
    the title field does not match the pattern"""
    invalid_top = replace(valid_prsv_top, title="M12345")
    invalid_contents = replace(valid_prsv_contents, title="M12345_")
    invalid_metadata = replace(valid_prsv_metadata, title="M12345_ER")

    assert not ingest_validator.validate_so_title(invalid_top, r"M[0-9]+_(ER|DI|EM)_[0-9]+")
    assert not ingest_validator.validate_so_title(invalid_contents, r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents")
    assert not ingest_validator.validate_so_title(invalid_metadata, r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata")



"""
1. response code should be good (200)
2. data structure should be as expected
"""
