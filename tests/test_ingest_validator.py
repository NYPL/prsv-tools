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


@dataclass
class prsv_Information_Object:
    uuid: str
    title: str
    type: str
    securityTag: str
    ioCategory: str


@pytest.fixture
def valid_prsv_top():
    prsv_top = prsv_Structural_Object(
        uuid="70ecde98-d40e-4a6f-b5e4-dd6dda34443d",
        title="M23385_ER_11",
        type="soCategory",
        securityTag="open",
        soCategory="ERContainer",
        mdFragments={"speccolID": "M23385"},
        children={
            "M23385_ER_11_contents": {
                "objType": "SO",
                "uuid": "70e2f9b8-10e7-4cc6-95cf-78755d03dfd7",
            },
            "M23385_ER_11_metadata": {
                "objType": "SO",
                "uuid": "d2bb302c-5e1b-477e-89ab-4436af786c53",
            },
        },
    )
    return prsv_top


@pytest.fixture
def valid_prsv_contents():
    prsv_contents = prsv_Structural_Object(
        uuid="70e2f9b8-10e7-4cc6-95cf-78755d03dfd7",
        title="M23385_ER_11_contents",
        type="soCategory",
        securityTag="open",
        soCategory="ERContents",
        mdFragments={
            "erNumber": "ER_11",
            "faCollectionId": "M23385",
            "faComponentId": "M23385_ER_11",
        },
        children={
            "[root].12": {
                "objType": "SO",
                "uuid": "5ba09c07-7420-4557-b098-0bca94b59378",
            },
        },
    )
    return prsv_contents


@pytest.fixture
def valid_prsv_metadata():
    prsv_metadata = prsv_Structural_Object(
        uuid="5df3566c-36f0-4721-9e5e-bc1c824b5910",
        title="M1126_DI_1_metadata",
        type="soCategory",
        securityTag="preservation",
        soCategory="DIMetadata",
        mdFragments=None,
        children={
            "M1126-0046p001.JPG": {
                "objType": "IO",
                "uuid": "359548a5-f1ee-4c47-a0ee-992ff0c7597d",
            }
        },
    )
    return prsv_metadata


@pytest.fixture
def valid_prsv_contents_information_object():
    prsv_contents_io = prsv_Information_Object(
        uuid="267bea1b-5f42-4c85-953c-c5127758df85",
        title="angels logo.eps",
        type="ioCategory",
        securityTag="open",
        ioCategory="ERElement",
    )
    return prsv_contents_io


@pytest.fixture
def valid_prsv_contents_structural_object():
    prsv_contents_so = prsv_Structural_Object(
        uuid="5ba09c07-7420-4557-b098-0bca94b59378",
        title="[root].12",
        type="soCategory",
        securityTag="open",
        soCategory="ERElement",
        mdFragments=None,
        children={
            "HULBERT": {
                "objType": "IO",
                "uuid": "aa69acd3-b54c-49c3-b01c-90e0a8b176cd",
            },
            "HULBERT.BAK": {
                "objType": "IO",
                "uuid": "43761244-678b-44ee-b339-67a253c5f781",
            },
        },
    )
    return prsv_contents_so


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


def test_get_top_so(valid_prsv_top):
    """test that get_so function returns the correct
    data class structure for the top level SO"""

    actual_api_value = ingest_validator.get_so(
        valid_prsv_top.uuid, token, namespaces, "top"
    )

    assert asdict(valid_prsv_top) == asdict(actual_api_value)


def test_get_contents_so(valid_prsv_contents):
    """test that get_so function returns the correct
    data class structure for the contents SO"""

    actual_api_value = ingest_validator.get_so(
        valid_prsv_contents.uuid, token, namespaces, "contents"
    )

    assert asdict(valid_prsv_contents) == asdict(actual_api_value)


def test_get_metadata_so(valid_prsv_metadata):
    """test that get_so function returns the correct
    data class structure for the contents SO"""

    actual_api_value = ingest_validator.get_so(
        valid_prsv_metadata.uuid, token, namespaces, "metadata"
    )

    assert asdict(valid_prsv_metadata) == asdict(actual_api_value)


@pytest.mark.parametrize(
    "expected_result, fixture_name, pattern",
    [
        (True, "valid_prsv_top", r"M[0-9]+_(ER|DI|EM)_[0-9]+"),
        (True, "valid_prsv_contents", r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents"),
        (True, "valid_prsv_metadata", r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata"),
        (False, "valid_prsv_top", r"M[0-9]+_(ER|DI|EM)_[0-9]+"),
        (False, "valid_prsv_contents", r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents"),
        (False, "valid_prsv_metadata", r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata"),
    ],
)
def test_validate_so_title(expected_result, fixture_name, pattern, request):
    """Test validate_so_title with different inputs and patterns"""

    input_data = request.getfixturevalue(fixture_name)

    if expected_result:
        assert ingest_validator.validate_so_title(input_data, pattern)
    else:
        invalid_input = replace(input_data, title="M00000")
        assert not ingest_validator.validate_so_title(invalid_input, pattern)


@pytest.mark.parametrize(
    "expected_result, fixture_name, string",
    [
        (True, "valid_prsv_top", "open"),
        (True, "valid_prsv_contents", "open"),
        (True, "valid_prsv_metadata", "preservation"),
        (False, "valid_prsv_top", "open"),
        (False, "valid_prsv_contents", "open"),
        (False, "valid_prsv_metadata", "preservation"),
    ],
)
def test_valid_sectag(expected_result, fixture_name, string, request):
    input_data = request.getfixturevalue(fixture_name)

    if expected_result:
        assert ingest_validator.valid_sectag(input_data, string)
    else:
        invalid_input = replace(input_data, securityTag="public")
        assert not ingest_validator.valid_sectag(invalid_input, string)


@pytest.mark.parametrize(
    "expected_result, fixture_name",
    [
        (True, "valid_prsv_top"),
        (True, "valid_prsv_contents"),
        (True, "valid_prsv_metadata"),
        (False, "valid_prsv_top"),
        (False, "valid_prsv_contents"),
        (False, "valid_prsv_metadata"),
    ],
)
def test_valid_so_type(expected_result, fixture_name, request):
    input_data = request.getfixturevalue(fixture_name)

    if expected_result:
        assert ingest_validator.valid_so_type(input_data)
    else:
        invalid_input = replace(input_data, type="ioCategory")
        assert not ingest_validator.valid_so_type(invalid_input)


@pytest.mark.parametrize(
    "expected_result, fixture_name, pkg_type, expected_category",
    [
        (True, "valid_prsv_top", "ER", "Container"),
        (True, "valid_prsv_contents", "ER", "Contents"),
        (True, "valid_prsv_metadata", "DI", "Metadata"),
        (True, "valid_prsv_contents_structural_object", "ER", "Element"),
        (False, "valid_prsv_top", "ER", "Container"),
        (False, "valid_prsv_contents", "ER", "Contents"),
        (False, "valid_prsv_metadata", "DI", "Metadata"),
        (False, "valid_prsv_contents_structural_object", "ER", "Element"),
    ],
)
def test_valid_soCategory(
    expected_result, fixture_name, pkg_type, expected_category, request
):
    input_data = request.getfixturevalue(fixture_name)

    if expected_result:
        assert ingest_validator.valid_soCategory(
            input_data, pkg_type, expected_category
        )
    else:
        invalid_input = replace(input_data, soCategory="category")
        assert not ingest_validator.valid_soCategory(
            invalid_input, pkg_type, expected_category
        )


@pytest.mark.parametrize(
    "fixture_name, key, value",
    [
        ("valid_prsv_top", "speccolID", "M23385"),
        ("valid_prsv_contents", "erNumber", "ER_11"),
        ("valid_prsv_contents", "faCollectionId", "M23385"),
        ("valid_prsv_contents", "faComponentId", "M23385_ER_11"),
    ],
)
def test_validate_mdfrag(fixture_name, key, value, request):
    """Test validate_mdfrag with different inputs and expected results"""

    input_data = request.getfixturevalue(fixture_name)

    assert ingest_validator.validate_mdfrag(input_data, key, value)


@pytest.mark.parametrize(
    "fixture_name, key, incorrect_value, correct_value",
    [
        ("valid_prsv_top", "speccolID", "M1234", "M24468"),
        ("valid_prsv_contents", "erNumber", "ER_100", "ER_8"),
        ("valid_prsv_contents", "faCollectionId", "M1111", "M24468"),
        ("valid_prsv_contents", "faComponentId", "M1111_ER_12", "M24468_ER_8"),
    ],
)
def test_validate_incorrect_mdfrag(
    fixture_name, key, incorrect_value, correct_value, request
):
    """Test validate_mdfrag with different incorrect inputs"""

    invalid_data = replace(
        request.getfixturevalue(fixture_name), mdFragments={key: incorrect_value}
    )
    assert not ingest_validator.validate_mdfrag(invalid_data, key, correct_value)


"""
get_contents_io_so
validate_contents_element_title
validate_io_type
valid_ioCategory
"""
"""
1. response code should be good (200)
2. data structure should be as expected
"""
