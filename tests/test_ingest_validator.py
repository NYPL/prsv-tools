import json
import re
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, replace
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

    mock_call_api_value = prsv_Structural_Object(
        uuid="154c2634-d22c-4b85-a9c6-819184551d17",
        title="M1126_DI_1",
        type="soCategory",
        securityTag="open",
        soCategory="DIContainer",
        mdFragments={"speccolID": "M1126"},
        children={
            "M1126_DI_1_contents": {
                "objType": "SO",
                "uuid": "9885feb6-3340-4a02-8585-e0f75f55eb92",
            },
            "M1126_DI_1_metadata": {
                "objType": "SO",
                "uuid": "5df3566c-36f0-4721-9e5e-bc1c824b5910",
            },
        },
    )
    actual_api_value = ingest_validator.get_so(
        mock_call_api_value.uuid, token, namespaces, "top"
    )

    assert asdict(mock_call_api_value) == asdict(actual_api_value)


def test_mock_get_contents_so():
    """test that get_so function returns the correct
    data class structure for the contents SO"""

    mock_call_api_value = prsv_Structural_Object(
        uuid="605d7c11-ce9a-4536-9b05-e8cba3843e15",
        title="M1126_ER_12_contents",
        type="soCategory",
        securityTag="open",
        soCategory="ERContents",
        mdFragments={
            "erNumber": "ER_12",
            "faCollectionId": "M1126",
            "faComponentId": "M1126_ER_12",
        },
        children={
            "Feedback Form--Bldg on Diversit": {
                "objType": "IO",
                "uuid": "909d7fc0-faec-4520-8719-567a393ddb19",
            },
            "Feedback--VSCC": {
                "objType": "IO",
                "uuid": "68381917-ba8e-478d-9453-3ea0d2251e94",
            },
            "Feedback--VSCC 2nd DRAFT": {
                "objType": "IO",
                "uuid": "dbb5c7e4-eb38-45fe-9a73-0530b78d3252",
            }
        }
    )
    actual_api_value = ingest_validator.get_so(
        mock_call_api_value.uuid, token, namespaces, "contents"
    )

    assert asdict(mock_call_api_value) == asdict(actual_api_value)


def test_mock_get_metadata_so():
    """test that get_so function returns the correct
    data class structure for the contents SO"""

    mock_call_api_value = prsv_Structural_Object(
        uuid="be5f6a75-a192-4e33-82ac-8cd5def54858",
        title="M1126_ER_16_metadata",
        type="soCategory",
        securityTag="preservation",
        soCategory="ERMetadata",
        mdFragments=None,
        children={
            "M1126_ER_16.tsv": {
                "objType": "IO",
                "uuid": "c1718e09-dcb1-4b52-9d77-8d7d0282c347",
            }
        },
    )
    actual_api_value = ingest_validator.get_so(
        mock_call_api_value.uuid, token, namespaces, "metadata"
    )

    assert asdict(mock_call_api_value) == asdict(actual_api_value)


@pytest.fixture
def valid_prsv_top():
    prsv_top = prsv_Structural_Object(
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
    return prsv_top


@pytest.fixture
def valid_prsv_contents():
    prsv_contents = prsv_Structural_Object(
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
        },
    )
    return prsv_contents


@pytest.fixture
def valid_prsv_metadata():
    prsv_metadata = prsv_Structural_Object(
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
    return prsv_metadata

@pytest.mark.parametrize(
    "fixture_name, pattern",
    [
        ("valid_prsv_top", r"M[0-9]+_(ER|DI|EM)_[0-9]+"),
        ("valid_prsv_contents", r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents"),
        ("valid_prsv_metadata", r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata"),
    ]
)
def test_validate_so_title(fixture_name, pattern, request):
    """Test that validate_so_title returns True when
    the title field matches the pattern"""

    input_data = request.getfixturevalue(fixture_name)
    assert ingest_validator.validate_so_title(input_data, pattern)

@pytest.mark.parametrize(
    "fixture_name, pattern",
    [
        ("valid_prsv_top", r"M[0-9]+_(ER|DI|EM)_[0-9]+"),
        ("valid_prsv_contents", r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents"),
        ("valid_prsv_metadata", r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata"),
    ]
)
def test_validate_incorrect_so_title(fixture_name, pattern, request):
    """test that validate_so_title returns False when
    the title field does not match the pattern"""

    input_data = request.getfixturevalue(fixture_name)
    invalid_input = replace(input_data, title="random_value")
    assert not ingest_validator.validate_so_title(invalid_input, pattern)


def test_invalid_so_title(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that validate_so_title returns False when
    the title field does not match the pattern"""
    invalid_top = replace(valid_prsv_top, title="M12345")
    invalid_contents = replace(valid_prsv_contents, title="M12345_")
    invalid_metadata = replace(valid_prsv_metadata, title="M12345_ER")

    assert not ingest_validator.validate_so_title(
        invalid_top, r"M[0-9]+_(ER|DI|EM)_[0-9]+"
    )
    assert not ingest_validator.validate_so_title(
        invalid_contents, r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents"
    )
    assert not ingest_validator.validate_so_title(
        invalid_metadata, r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata"
    )

def test_valid_sectag(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that valid_sectag returns True when
    the security tag field (securityTag) matches the string value"""

    assert ingest_validator.valid_sectag(valid_prsv_top, "open")
    assert ingest_validator.valid_sectag(valid_prsv_contents, "open")
    assert ingest_validator.valid_sectag(valid_prsv_metadata, "preservation")

def test_invalid_sectag(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that valid_sectag returns False when
    the security tag field (securityTag) does not match the string value"""
    invalid_top = replace(valid_prsv_top, securityTag="closed")
    invalid_contents = replace(valid_prsv_contents, securityTag="closed")
    invalid_metadata = replace(valid_prsv_metadata, securityTag="open")

    assert not ingest_validator.valid_sectag(invalid_top, "open")
    assert not ingest_validator.valid_sectag(invalid_contents, "open")
    assert not ingest_validator.valid_sectag(invalid_metadata, "preservation")

def test_valid_so_type(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that valid_sectag returns True when
    the type field is soCategory"""

    assert ingest_validator.valid_so_type(valid_prsv_top)
    assert ingest_validator.valid_so_type(valid_prsv_contents)
    assert ingest_validator.valid_so_type(valid_prsv_metadata)

def test_invalid_so_type(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that valid_sectag returns False when
    the type field is not soCategory"""
    invalid_top = replace(valid_prsv_top, type="ioCategory")
    invalid_contents = replace(valid_prsv_contents, type="ioCategory")
    invalid_metadata = replace(valid_prsv_metadata, type="ioCategory")

    assert not ingest_validator.valid_so_type(invalid_top)
    assert not ingest_validator.valid_so_type(invalid_contents)
    assert not ingest_validator.valid_so_type(invalid_metadata)

def test_valid_soCategory(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that valid_soCategory returns True when
    the soCategory is as an expected string value"""

    assert ingest_validator.valid_soCategory(valid_prsv_top, "Container")
    assert ingest_validator.valid_soCategory(valid_prsv_contents, "Contents")
    assert ingest_validator.valid_soCategory(valid_prsv_metadata, "Metadata")

def test_invalid_soCategory(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that valid_soCategory returns False when
    the soCategory is as an unexpected string value"""
    invalid_top = replace(valid_prsv_top, soCategory="ERContents")
    invalid_contents = replace(valid_prsv_contents, soCategory="DIMetadata")
    invalid_metadata = replace(valid_prsv_metadata, soCategory="ERContainer")

    assert not ingest_validator.valid_soCategory(invalid_top, "Container")
    assert not ingest_validator.valid_soCategory(invalid_contents, "Contents")
    assert not ingest_validator.valid_soCategory(invalid_metadata, "Metadata")

def test_validate_mdfrag(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that validate_mdfrag return True
    if it is an expected dictionary"""

    assert ingest_validator.validate_mdfrag(valid_prsv_top, "speccolID", "M24468")
    assert ingest_validator.validate_mdfrag(valid_prsv_contents, "erNumber", "ER_8")
    assert ingest_validator.validate_mdfrag(valid_prsv_contents, "faCollectionId", "M24468")
    assert ingest_validator.validate_mdfrag(valid_prsv_contents, "faComponentId", "M24468_ER_8")
    # how to test for valid_prsv_metadata's mdfrag, which is None



def test_validate_incorrect_mdfrag(valid_prsv_top, valid_prsv_contents, valid_prsv_metadata):
    """test that validate_mdfrag return False
    if it is not an expected dictionary"""

    invalid_top = replace(valid_prsv_top, mdFragments={'speccolID': 'M1234'})
    invalid_contents = replace(valid_prsv_contents,
                               mdFragments={'erNumber': 'ER_100',
                                    'faCollectionId': 'M1111',
                                    'faComponentId': 'M1111_ER_12'})

    assert not ingest_validator.validate_mdfrag(invalid_top, "speccolID", "M24468")
    assert not ingest_validator.validate_mdfrag(invalid_contents, "erNumber", "ER_8")
    assert not ingest_validator.validate_mdfrag(invalid_contents, "faCollectionId", "M24468")
    assert not ingest_validator.validate_mdfrag(invalid_contents, "fa", "M24468_ER_8")


"""
1. response code should be good (200)
2. data structure should be as expected
"""
