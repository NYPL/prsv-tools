import json
from dataclasses import asdict, dataclass, replace

import pytest
from schema import Regex, Schema

import prsv_tools.ingest.validate_ingest as validate_ingest
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
    "spec_ns": "{{http://nypl.org/prsv_schemas/specCollection}}",
    "fa_ns": "{{http://nypl.org/prsv_schemas/findingAid}}",
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
    res = validate_ingest.get_api_results(token, url)
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


@pytest.fixture
def valid_prsv_contents_so_element_list():
    so_element_list = [
        prsv_Structural_Object(
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
    ]
    return so_element_list


@pytest.fixture
def valid_prsv_contents_io_element_list():
    io_element_list = [
        prsv_Information_Object(
            uuid="aa69acd3-b54c-49c3-b01c-90e0a8b176cd",
            title="HULBERT",
            type="ioCategory",
            securityTag="open",
            ioCategory="ERElement",
        ),
        prsv_Information_Object(
            uuid="43761244-678b-44ee-b339-67a253c5f781",
            title="HULBERT.BAK",
            type="ioCategory",
            securityTag="open",
            ioCategory="ERElement",
        ),
    ]
    return io_element_list


@pytest.fixture
def valid_prsv_metadata_io_ftk():
    metadata_io = prsv_Information_Object(
        uuid="2236cc8d-b38f-4167-96c1-abc953621c20",
        title="M1126_ER_10.tsv",
        type="ioCategory",
        securityTag="preservation",
        ioCategory="FTK report",
    )
    return metadata_io


@pytest.fixture
def valid_prsv_metadata_io_jpg():
    metadata_io = prsv_Information_Object(
        uuid="359548a5-f1ee-4c47-a0ee-992ff0c7597d",
        title="M1126-0046p001.JPG",
        type="ioCategory",
        securityTag="preservation",
        ioCategory="Carrier photograph",
    )
    return metadata_io


# unit tests
def test_content_searchwithin_so_endpoint():
    """test that the response text has the conceived structure,
    which is a non-empty list consisting of UUID(s)"""
    response = validate_ingest.search_within_DigArch(token, fields, test_digarch_uuid)

    uuid_ls = validate_ingest.parse_structural_object_uuid(response)

    expected_schema = Schema(
        [Regex(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")]
    )

    assert expected_schema.is_valid(uuid_ls)


def test_get_top_so(valid_prsv_top):
    """test that get_so function returns the correct
    data class structure for the top level SO"""

    actual_api_value = validate_ingest.get_so(
        valid_prsv_top.uuid, token, namespaces, "top"
    )

    assert asdict(valid_prsv_top) == asdict(actual_api_value)


def test_get_contents_so(valid_prsv_contents):
    """test that get_so function returns the correct
    data class structure for the contents SO"""

    actual_api_value = validate_ingest.get_so(
        valid_prsv_contents.uuid, token, namespaces, "contents"
    )

    assert asdict(valid_prsv_contents) == asdict(actual_api_value)


def test_get_metadata_so(valid_prsv_metadata):
    """test that get_so function returns the correct
    data class structure for the contents SO"""

    actual_api_value = validate_ingest.get_so(
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
        assert validate_ingest.validate_so_title(input_data, pattern)
    else:
        invalid_input = replace(input_data, title="M00000")
        assert not validate_ingest.validate_so_title(invalid_input, pattern)


@pytest.mark.parametrize(
    "expected_result, fixture_name, string",
    [
        (True, "valid_prsv_top", "open"),
        (True, "valid_prsv_contents", "open"),
        (True, "valid_prsv_metadata", "preservation"),
        (True, "valid_prsv_contents_information_object", "open"),
        (True, "valid_prsv_contents_structural_object", "open"),
        (True, "valid_prsv_metadata_io_ftk", "preservation"),
        (True, "valid_prsv_metadata_io_jpg", "preservation"),
        (False, "valid_prsv_top", "open"),
        (False, "valid_prsv_contents", "open"),
        (False, "valid_prsv_metadata", "preservation"),
        (False, "valid_prsv_contents_information_object", "open"),
        (False, "valid_prsv_contents_structural_object", "open"),
        (False, "valid_prsv_metadata_io_ftk", "preservation"),
        (False, "valid_prsv_metadata_io_jpg", "preservation"),
    ],
)
def test_valid_sectag(expected_result, fixture_name, string, request):
    """test that valid_sectag will return expected result with different prsv fixtures"""
    input_data = request.getfixturevalue(fixture_name)

    if expected_result:
        assert validate_ingest.valid_sectag(input_data, string)
    else:
        invalid_input = replace(input_data, securityTag="public")
        assert not validate_ingest.valid_sectag(invalid_input, string)


@pytest.mark.parametrize(
    "expected_result, fixture_name",
    [
        (True, "valid_prsv_top"),
        (True, "valid_prsv_contents"),
        (True, "valid_prsv_metadata"),
        (True, "valid_prsv_contents_structural_object"),
        (False, "valid_prsv_top"),
        (False, "valid_prsv_contents"),
        (False, "valid_prsv_metadata"),
        (False, "valid_prsv_contents_structural_object"),
    ],
)
def test_valid_so_type(expected_result, fixture_name, request):
    """test valid_so_type will return expected result with different prsv SO fixtures"""
    input_data = request.getfixturevalue(fixture_name)

    if expected_result:
        assert validate_ingest.valid_so_type(input_data)
    else:
        invalid_input = replace(input_data, type="ioCategory")
        assert not validate_ingest.valid_so_type(invalid_input)


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
    """test valid_soCategory will return expected result using different fixtures and
    expected strings"""
    input_data = request.getfixturevalue(fixture_name)

    if expected_result:
        assert validate_ingest.valid_soCategory(input_data, pkg_type, expected_category)
    else:
        invalid_input = replace(input_data, soCategory="category")
        assert not validate_ingest.valid_soCategory(
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

    assert validate_ingest.validate_mdfrag(input_data, key, value)


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
    assert not validate_ingest.validate_mdfrag(invalid_data, key, correct_value)


@pytest.mark.parametrize(
    "expected_result, fixture_name",
    [
        (True, "valid_prsv_contents_information_object"),
        (True, "valid_prsv_contents_structural_object"),
        (False, "valid_prsv_contents_information_object"),
        (False, "valid_prsv_contents_structural_object"),
    ],
)
def test_validate_contents_element_title(expected_result, fixture_name, request):
    """test validate_contents_element_title will return expected return using different fixtures"""
    if expected_result:
        input_data = request.getfixturevalue(fixture_name)
        assert validate_ingest.validate_contents_element_title(input_data)
    else:
        invalid_data = replace(request.getfixturevalue(fixture_name), title=None)
        assert not validate_ingest.validate_contents_element_title(invalid_data)


def test_validate_io_type(valid_prsv_contents_information_object):
    """test validate_io_type return True with valid contents information object"""
    assert validate_ingest.validate_io_type(valid_prsv_contents_information_object)


def test_invalid_io_type(valid_prsv_contents_information_object):
    """test validate_io_type return False with invalid contents information object"""
    invalid_data = replace(valid_prsv_contents_information_object, type="soCategory")
    assert not validate_ingest.validate_io_type(invalid_data)


def test_valid_contents_ioCategory(valid_prsv_contents_information_object):
    """test valid_contents_ioCategory returns True with correct input"""
    assert validate_ingest.valid_contents_ioCategory(
        valid_prsv_contents_information_object, "ER"
    )


def test_invalid_contents_ioCategory(valid_prsv_contents_information_object):
    """test valid_contents_ioCategory returns False with incorrect input"""
    invalid_data = replace(
        valid_prsv_contents_information_object, ioCategory="DIElement"
    )
    assert not validate_ingest.valid_contents_ioCategory(invalid_data, "ER")


@pytest.mark.parametrize(
    "expected_result, fixture_name",
    [
        (True, "valid_prsv_metadata_io_ftk"),
        (True, "valid_prsv_metadata_io_jpg"),
        (False, "valid_prsv_metadata_io_ftk"),
    ],
)
def test_valid_metadata_ioCategory(expected_result, fixture_name, request):
    """test valid_metadata_ioCategory will return expected value with different
    fixtures and inputs"""
    if expected_result:
        input_data = request.getfixturevalue(fixture_name)
        assert validate_ingest.valid_metadata_ioCategory(input_data)
    else:
        invalid_data = replace(request.getfixturevalue(fixture_name), ioCategory="FTK")
        assert not validate_ingest.valid_metadata_ioCategory(invalid_data)


@pytest.fixture
def er_on_fs(tmp_path):
    er_path = tmp_path / "M23385_ER_11"
    content_path = er_path / "objects" / "[root].12"
    content_path.mkdir(parents=True)
    (content_path / "HULBERT.BAK").touch()
    (content_path / "HULBERT").touch()
    md_path = er_path / "metadata"
    md_path.mkdir()
    (md_path / "M1126_ER_10.tsv").touch()
    (md_path / "M1126-0046p001.JPG")

    return er_path


def test_valid_content_count(valid_prsv_contents, er_on_fs):
    assert validate_ingest.valid_content_count(valid_prsv_contents, er_on_fs)


def test_invalid_count_uningested_file(valid_prsv_contents, er_on_fs):
    (er_on_fs / "objects" / "uningested_file").touch()
    assert not validate_ingest.valid_content_count(valid_prsv_contents, er_on_fs)


def test_invalid_count_new_file(valid_prsv_contents, er_on_fs):
    (er_on_fs / "objects" / "[root].12" / "HULBERT").unlink()
    assert not validate_ingest.valid_content_count(valid_prsv_contents, er_on_fs)


def test_valid_content_filenames_retained(valid_prsv_contents, er_on_fs):
    assert validate_ingest.valid_content_filenames(valid_prsv_contents, er_on_fs)


def test_invalid_content_filenames_changed(valid_prsv_contents, er_on_fs):
    renamed = er_on_fs / "objects" / "[root].12" / "HULBERT"
    renamed.rename(renamed.with_suffix(".STRIPPED_EXTENSION"))
    assert not validate_ingest.valid_content_filenames(valid_prsv_contents, er_on_fs)
