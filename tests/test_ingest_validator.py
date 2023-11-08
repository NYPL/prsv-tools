import json
import re
import xml.etree.ElementTree as ET
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
    "fa_ns": f"{{http://nypl.org/prsv_schemas/findingAid}}"
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
        {Regex(r".+"): {"objType": Or("SO", "IO"),
         "uuid": Regex(f"{uuid_pattern}")}}
    )
    assert child_schema.is_valid(contents_so_dataclass.children)


# def test_get_so_metadata():
#     """test that get_so function returns the correct
#     data class structure for the metadata SO"""

#     metadata_so_dataclass = ingest_validator.get_so(
#         test_metadata_uuid, token, namespaces, "metadata"
#     )
#     assert re.fullmatch(uuid_pattern, metadata_so_dataclass.uuid)
#     assert re.fullmatch(
#         r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata", metadata_so_dataclass.title
#     )
#     assert metadata_so_dataclass.type == "soCategory"
#     assert metadata_so_dataclass.securityTag == "preservation"
#     assert metadata_so_dataclass.soCategory in [
#         "ERMetadata",
#         "DIMetadata",
#         "EMMetadata",
#     ]
#     assert metadata_so_dataclass.mdFragments == None
#     assert isinstance(metadata_so_dataclass.children, dict)

#     so_schema = Schema(
#         {
#             "title": Regex(r"M[0-9]+_(ER|DI|EM)_[0-9]+"),
#             "sectag": Or("open", "preservation"),
#             "id_url": Regex(
#                 rf"^https://nypl.preservica.com/api/entity/structural-objects/{uuid_pattern}/identifiers$"
#             ),
#             "metadata_url": Regex(
#                 rf"^https://nypl.preservica.com/api/entity/structural-objects/{uuid_pattern}/metadata/{uuid_pattern}$"
#             ),
#             "children_url": Regex(
#                 rf"^https://nypl.preservica.com/api/entity/structural-objects/{uuid_pattern}/children$"
#             ),
#         }
#     )

#     global er_dict
#     er_dict = ingest_validator.get_so_metadata(test_er_uuid, token, namespaces)

#     assert so_schema.is_valid(er_dict) == True


# def test_get_so_identifier():
#     """test that get_so_identifier returns a dictionary with type (str)
#     and SO category (soCat) (str)"""

#     id_schema = Schema(
#         {"type": "soCategory", "soCat": Or("DIContainer", "ERContainer", "EMContainer")}
#     )
#     id_dict = ingest_validator.get_so_identifier(token, er_dict, namespaces)

#     assert id_schema.is_valid(id_dict) == True


# def test_get_spec_mdfrag():
#     """test that get_spec_mdfrag returns a dictionary with
#     SPEC collection ID"""

#     speccol_schema = Schema({"speccolID": Regex(r"M[0-9]+")})

#     spec_dict = ingest_validator.get_spec_mdfrag(token, er_dict, namespaces)

#     assert speccol_schema.is_valid(spec_dict) == True


# def test_get_so_children():
#     """test that get_so_children returns a dictionary with
#     children as the key and a list of url(s) as its value"""

#     children_schema = Schema(
#         {
#             "children": [
#                 Regex(
#                     rf"https://nypl.preservica.com/api/entity/structural-objects/{uuid_pattern}"
#                 )
#             ]
#         }
#     )
#     children_dict = ingest_validator.get_so_children(token, er_dict, namespaces)

#     assert children_schema.is_valid(children_dict) == True


"""
1. response code should be good (200)
2. data structure should be as expected
"""
