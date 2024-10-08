import json
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import requests

import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.cli as prsvcli

logging.basicConfig(level=logging.INFO)


def parse_args():
    parser = prsvcli.Parser()

    parser.add_argument(
        "--credentials",
        type=str,
        required=True,
        choices=["test-ingest", "prod-ingest", "test-manage"],
        help="which set of credentials to use",
    )

    parser.add_argument(
        "--collectionID",
        type=str,
        required=True,
        help="the collection you'd like to check for, M\\d+",
    )

    parser.add_argument(
        "--source",
        type=str,
        required=True,
        help="the source directory you want to compare to, usually ICA path to 'faComponents'",
    )

    return parser.parse_args()


@dataclass
class datamodel_Structural_Object:
    title: str
    type: str
    securityTag: str
    soCategory: str
    mdFragments: dict | None
    children: dict | None


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


def get_api_results(credentials: str, url: str) -> requests.Response:
    """function to get api results"""
    accesstoken = prsvapi.get_token(credentials)
    headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml",
    }
    response = requests.request("GET", url, headers=headers)
    return response


def search_within_DigArch(
    credentials: str, fields, parentuuid: str
) -> requests.Response:
    """function to search within the DigArch folder in Preservica with
    predefined queried fields"""
    query = {"q": "", "fields": fields}
    q = json.dumps(query)
    url = f"https://nypl.preservica.com/api/content/search-within?q={q}&parenthierarchy={parentuuid}&start=0&max=-1&metadata=''"  # noqa
    res = get_api_results(credentials, url)

    return res


def parse_structural_object_uuid(res: requests.Response) -> list:
    """function to parse json API response into a list of UUIDs"""
    uuid_ls = list()
    json_obj = json.loads(res.text)
    obj_ids = json_obj["value"]["objectIds"]
    for sdbso in obj_ids:
        uuid_ls.append(sdbso[-36:])

    return uuid_ls


def ingest_has_correct_ER_number(
    collection_id: str, da_source: Path, uuid_ls: list
) -> bool:
    """function to verify inquired collection has the correct number of packages
    in Preservica. Return False if not."""
    col_folder = da_source / collection_id
    pkgs = [
        x
        for x in col_folder.iterdir()
        if x.is_dir()
        and x.name.startswith(collection_id)
        and not x.name.endswith("photographs")
    ]
    expected = len(pkgs)
    found = len(uuid_ls)

    if expected == found:
        logging.info(
            f"""{collection_id} has correct number of packages in Preservica
                 PRSV has {found}
                 source has {expected}"""
        )
        return True
    else:
        logging.error(
            f"""{collection_id} has incorrect number of packages in Preservica
                      PRSV has {found}
                      source has {expected}"""
        )
        return False


def get_so(
    uuid: str, credentials: str, namespaces: dict, so_type: str
) -> prsv_Structural_Object:
    """function to parse API result and return a prsv_Structural_Object data class object"""
    url = f"https://nypl.preservica.com/api/entity/structural-objects/{uuid}"
    res = get_api_results(credentials, url)
    root = ET.fromstring(res.text)

    uuid = root.find(f".//{namespaces['xip_ns']}Ref").text

    title = root.find(f".//{namespaces['xip_ns']}Title").text

    sectag = root.find(f".//{namespaces['xip_ns']}SecurityTag").text

    identifiers_url = root.find(f".//{namespaces['entity_ns']}Identifiers").text

    identifiers_res = get_api_results(credentials, identifiers_url)
    id_root = ET.fromstring(identifiers_res.text)

    type = id_root.find(f".//{namespaces['xip_ns']}Type").text
    soCat = id_root.find(f".//{namespaces['xip_ns']}Value").text

    if so_type in ["metadata", "contents_element"]:
        md = None
    else:
        metadata_url = root.find(f".//{namespaces['entity_ns']}Fragment").text

    if so_type == "top":
        md = get_spec_mdfrag(credentials, metadata_url, namespaces)
    elif so_type == "contents":
        md = get_fa_mdfrag(credentials, metadata_url, namespaces)

    children = get_so_children(credentials, uuid, namespaces)

    return prsv_Structural_Object(uuid, title, type, sectag, soCat, md, children)


def get_spec_mdfrag(credentials: str, metadata_url: str, namespaces: dict) -> dict:
    """function to get and parse API result for SPEC metadata and return a dictionary"""
    mdfrag_dict = dict()

    mfrag_res = get_api_results(credentials, metadata_url)
    mfrag_root = ET.fromstring(mfrag_res.text)

    speccolid_elem = mfrag_root.find(f".//{namespaces['spec_ns']}specCollectionId")
    mdfrag_dict["speccolID"] = speccolid_elem.text

    return mdfrag_dict


def get_fa_mdfrag(credentials: str, metadata_url: str, namespaces: dict) -> dict:
    """function to get and parse API result for Finding Aids metadata and
    return a dictionary"""
    mdfrag_dict = dict()

    mfrag_res = get_api_results(credentials, metadata_url)
    mfrag_root = ET.fromstring(mfrag_res.text)

    fa_component_id = mfrag_root.find(f".//{namespaces['fa_ns']}faComponentId")
    fa_collection_id = mfrag_root.find(f".//{namespaces['fa_ns']}faCollectionId")
    er_number = mfrag_root.find(f".//{namespaces['fa_ns']}erNumber")

    mdfrag_dict["faComponentId"] = fa_component_id.text
    mdfrag_dict["faCollectionId"] = fa_collection_id.text
    mdfrag_dict["erNumber"] = er_number.text

    return mdfrag_dict


def get_so_children(credentials: str, so_uuid: str, namespaces: dict) -> dict:
    """function to get and parse children field of a prsv Structural Object"""
    children_dict = dict()

    def process_children_root(root):
        children = root.findall(f".//{namespaces['entity_ns']}Child")
        for child in children:
            title = child.attrib.get("title")
            ref = child.attrib.get("ref")
            type = child.attrib.get("type")
            children_dict[title] = {"objType": type, "uuid": ref}

    children_url = f"https://nypl.preservica.com/api/entity/structural-objects/{so_uuid}/children?start=0&max=1000"  # noqa

    children_res = get_api_results(credentials, children_url)
    children_root = ET.fromstring(children_res.text)
    process_children_root(children_root)

    if len(children_root.findall(f".//{namespaces['entity_ns']}Next")) > 0:
        next_children = children_root.findall(f".//{namespaces['entity_ns']}Next")
        for next_child in next_children:
            next_url = next_child.text
            next_res = get_api_results(credentials, next_url)
            next_root = ET.fromstring(next_res.text)
            process_children_root(next_root)

    return children_dict


def get_io(uuid: str, credentials: str, namespaces: dict) -> prsv_Information_Object:
    """function to get and parse API to get an Information Object dataclass object"""
    url = f"https://nypl.preservica.com/api/entity/information-objects/{uuid}"
    res = get_api_results(credentials, url)
    root = ET.fromstring(res.text)

    uuid = root.find(f".//{namespaces['xip_ns']}Ref").text

    title = root.find(f".//{namespaces['xip_ns']}Title").text

    sectag = root.find(f".//{namespaces['xip_ns']}SecurityTag").text

    identifiers_url = root.find(f".//{namespaces['entity_ns']}Identifiers").text

    identifiers_res = get_api_results(credentials, identifiers_url)
    id_root = ET.fromstring(identifiers_res.text)

    type = id_root.find(f".//{namespaces['xip_ns']}Type").text
    ioCat = id_root.find(f".//{namespaces['xip_ns']}Value").text

    return prsv_Information_Object(uuid, title, type, sectag, ioCat)


def validate_so_title(so: prsv_Structural_Object, pattern: str) -> bool:
    """function to validate title pattern of a prsv Structural Object"""
    if re.fullmatch(pattern, so.title):
        return True
    else:
        logging.error(f"{so.title} does not conform to {pattern}")
        return False


def valid_sectag(
    io_so: prsv_Structural_Object | prsv_Information_Object, expected: str
) -> bool:
    """function to validate prsv Structural and Information Object security tag values"""
    if io_so.securityTag == expected:
        return True
    else:
        logging.error(f"Security tag is not {expected}, but {io_so.securityTag}")
        return False


def valid_so_type(so: prsv_Structural_Object) -> bool:
    """function to validate Structural Object type, which must be soCategory
    return True if so; False if not"""
    if so.type == "soCategory":
        return True
    else:
        logging.error(f"Type is not soCategory, but {so.type}")
        return False


def valid_soCategory(
    so: prsv_Structural_Object, pkg_type: str, expected_category: str
) -> bool:
    """function to validate soCategory value. Return True if it is as expected; False if not"""
    if so.soCategory == f"{pkg_type}{expected_category}":
        return True
    else:
        logging.error(f"{so.title} SO category is incorrect: {so.soCategory}")
        return False


def validate_mdfrag(
    prsv_object: prsv_Structural_Object, field_name: str, expected_value: str
) -> bool:
    """function to validate Structural Object metadata fragment values.
    Return True if it is as expected; False if not"""
    # mdFragments is a dictionary
    if prsv_object.mdFragments.get(field_name) == expected_value:
        return True
    else:
        logging.error(
            f"{prsv_object.title} has incorrect {field_name}: {prsv_object.mdFragments.get(field_name)}"
        )
        return False


def valid_top_level_mdfrag(
    top_level_so: prsv_Structural_Object, collectionId: str
) -> None:
    """function to run other function to validate top level folder
    metadata fragment field"""
    validate_mdfrag(top_level_so, "speccolID", collectionId)


def valid_contents_mdfrags(
    contents_so: prsv_Structural_Object, collectionId: str
) -> None:
    """function to run other functions to validate contents level folder
    metadata fragment fields"""
    fa_component_id = re.search(
        r"(M[0-9]+_(ER|DI|EM)_[0-9]+)_contents", contents_so.title
    ).group(1)

    er_number = re.search(
        r"M[0-9]+_((ER|DI|EM)_[0-9]+)_contents", contents_so.title
    ).group(1)

    validate_mdfrag(contents_so, "faComponentId", fa_component_id)
    validate_mdfrag(contents_so, "faCollectionId", collectionId)
    validate_mdfrag(contents_so, "erNumber", er_number)


def valid_all_top_level_so_conditions(
    top_level_so: prsv_Structural_Object, pkg_type: str, collectionId: str
) -> None:
    """function to run other functions to validate all top level folder
    requirements"""
    logging.info(f"validating top level {top_level_so.title}")
    validate_so_title(top_level_so, r"M[0-9]+_(ER|DI|EM)_[0-9]+")
    valid_sectag(top_level_so, "open")
    valid_so_type(top_level_so)
    valid_soCategory(top_level_so, pkg_type, "Container")
    valid_top_level_mdfrag(top_level_so, collectionId)


def valid_all_contents_level_so_conditions(
    contents_so: prsv_Structural_Object, pkg_type: str, collectionId: str
) -> None:
    """function to run other functions to validate all contents level folder
    requirements"""
    logging.info(f"validating contents level {contents_so.title}")
    validate_so_title(contents_so, r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents")
    valid_sectag(contents_so, "open")
    valid_so_type(contents_so)
    valid_soCategory(contents_so, pkg_type, "Contents")
    valid_contents_mdfrags(contents_so, collectionId)


def valid_all_metadata_level_so_conditions(
    metadata_so: prsv_Structural_Object, pkg_type: str
) -> None:
    """function to run other functions to validate all metadata level folder
    requirements"""
    logging.info(f"validating metadata level {metadata_so.title}")
    validate_so_title(metadata_so, r"M[0-9]+_(ER|DI|EM)_[0-9]+_metadata")
    valid_sectag(metadata_so, "preservation")
    valid_so_type(metadata_so)
    valid_soCategory(metadata_so, pkg_type, "Metadata")


def get_contents_io_so(
    so: list, credentials: str, namespaces: dict
) -> Tuple[List[prsv_Information_Object], List[prsv_Structural_Object]]:
    """function to get all the elements within the contents level SO,
    returning a list of IOs and a list of SOs, if applicable"""
    contents_io = []
    contents_element_so = []
    for child in so.children:
        uuid = so.children[child]["uuid"]
        if so.children[child]["objType"] == "IO":
            io = get_io(uuid, credentials, namespaces)
            contents_io.append(io)
        elif so.children[child]["objType"] == "SO":
            element_so = get_so(uuid, credentials, namespaces, "contents_element")
            contents_element_so.append(element_so)
            new_io, new_element_so = get_contents_io_so(
                element_so, credentials, namespaces
            )
            contents_io.extend(new_io)
            contents_element_so.extend(new_element_so)
    return contents_io, contents_element_so


def validate_contents_element_title(
    contents_element: prsv_Information_Object | prsv_Structural_Object,
) -> bool:
    """function to validate contents element (IO and SO) title.
    They must be string datatype"""
    if isinstance(contents_element.title, str):
        return True
    else:
        logging.error(f"{contents_element.title} is not a string")
        return False


def validate_io_type(io_element: prsv_Information_Object) -> bool:
    """function to validate IO type, which must be ioCategory.
    Return True if so; False if not"""
    if io_element.type == "ioCategory":
        return True
    else:
        logging.error(
            f"{io_element.title}'s type is not ioCategory, but {io_element.type}"
        )
        return False


def valid_contents_ioCategory(
    io_element: prsv_Information_Object, pkg_type: str
) -> bool:
    """function to validate contents ioCategory, which must be (DI|EM|ER)Element
    return True if so; False if not"""
    if io_element.ioCategory == f"{pkg_type}Element":
        return True
    else:
        logging.error(
            f"{io_element.title} has incorrect ioCategory: {io_element.ioCategory}"
        )
        return False


def valid_metadata_ioCategory(metadata_io: prsv_Information_Object) -> bool:
    """function to validate metadata ioCategory, which must be either FTK report
    or Carrier photograph, depending on the file extension"""
    if (
        Path(metadata_io.title).suffix.lower() == ".tsv"
        or Path(metadata_io.title).suffix.lower() == ".csv"
    ):
        if metadata_io.ioCategory == "FTK report":
            return True
    elif Path(metadata_io.title).suffix.lower() == ".jpg":
        if metadata_io.ioCategory == "Carrier photograph":
            return True
    else:
        logging.error(
            f"{metadata_io.title} has incorrect ioCategory: {metadata_io.ioCategory}"
        )
        return False


def validate_all_contents_element_io_conditions(
    io_element: prsv_Information_Object, pkg_type: str
) -> None:
    """function to run other functions to validate all contents element
    Information Object's conditions"""
    logging.info(f"validating {io_element.title}")
    validate_contents_element_title(io_element)
    validate_io_type(io_element)
    valid_sectag(io_element, "open")
    valid_contents_ioCategory(io_element, pkg_type)


def validate_all_contents_element_so_conditions(
    so_element: prsv_Structural_Object, pkg_type: str
) -> None:
    """function to run other functions to validate all contents element
    Structural Object's conditions"""
    logging.info(f"validating {so_element.title}")
    validate_contents_element_title(so_element)
    valid_so_type(so_element)
    valid_sectag(so_element, "open")
    valid_soCategory(so_element, pkg_type, "Element")


def validate_all_metadata_io_conditions(metadata_io: prsv_Information_Object) -> None:
    """function to run other functions to validate all metadata
    Information Object's conditions"""
    logging.info(f"validating {metadata_io.title}")
    validate_io_type(metadata_io)
    valid_sectag(metadata_io, "preservation")
    valid_metadata_ioCategory(metadata_io)


def get_contents_io_so_count(
    contents_io: list, contents_element_so: list
) -> Tuple[int, int]:
    """function to get the count of IOs in the PRSV contents SO
    and the count of SOs in the PRSV contents SO"""
    return len(contents_io), len(contents_element_so)


def get_source_file_folder_count(
    source: Path, collection_id: str, pkg_title: str
) -> Tuple[List, List]:
    """function to get file list and folder list of the source
    'objects' directory"""
    obj_path = source / collection_id / pkg_title / "objects"
    file_list = [
        x for x in obj_path.rglob("*") if x.is_file() and not x.name.startswith(".")
    ]
    folder_list = [y for y in obj_path.rglob("*") if y.is_dir()]

    return len(file_list), len(folder_list)


def valid_contents_count(
    contents_io_ct: int,
    contents_element_so_ct: int,
    source_file_ct: int,
    source_folder_ct: int,
) -> bool:
    """function to compare prsv contents folder IO count and SO count with the source file
    system's folder and file count"""
    if contents_io_ct == source_file_ct and contents_element_so_ct == source_folder_ct:
        logging.info(
            """IOs and SOs counts of the contents folder are the same as
                     the file system"""
        )
        return True
    else:
        logging.error(
            f"""Contents IO and/or SO count(s) incorrect
                          PRSV contents IO count: {contents_io_ct}
                          Source file count: {source_file_ct}
                          PRSV contents SO count: {contents_element_so_ct}
                          Source folder count: {source_folder_ct}"""
        )
        return False


def main():
    """
    First type of check:
    1. Total number in one collection (M12234_ER_1234)
    2. Total number of file within objects and metadata

    Second type of check:
    1. If it is an SO & fits certain naming convention, do xxxxx
    2. If it is an IO, distinguish between content file and metadata file
       to check different checkpoints
    """
    args = parse_args()

    if "test" in args.credentials:
        parentuuid = "c0b9b47a-5552-4277-874e-092b3cc53af6"
        version = prsvapi.find_apiversion(args.credentials)
        da_source = Path(args.source)

    else:
        parentuuid = "e80315bc-42f5-44da-807f-446f78621c08"
        version = prsvapi.find_apiversion(args.credentials)
        da_source = Path(args.source)

    namespaces = {
        "xip_ns": f"{{http://preservica.com/XIP/v{version}}}",
        "entity_ns": f"{{http://preservica.com/EntityAPI/v{version}}}",
        "spec_ns": "{http://nypl.org/prsv_schemas/specCollection}",
        "fa_ns": "{http://nypl.org/prsv_schemas/findingAid}",
    }

    fields_top = [{"name": "spec.specCollectionID", "values": [args.collectionID]}]
    res_uuid = search_within_DigArch(args.credentials, fields_top, parentuuid)
    uuid_ls = parse_structural_object_uuid(res_uuid)

    ingest_has_correct_ER_number(args.collectionID, da_source, uuid_ls)

    for uuid in uuid_ls:
        top_level_so = get_so(uuid, args.credentials, namespaces, "top")
        pkg_type = re.search(r"(ER|EM|DI)", top_level_so.title).group(0)
        contents_f = f"{top_level_so.title}_contents"
        metadata_f = f"{top_level_so.title}_metadata"
        contents_uuid = top_level_so.children[contents_f]["uuid"]
        contents_so = get_so(contents_uuid, args.credentials, namespaces, "contents")
        metadata_uuid = top_level_so.children[metadata_f]["uuid"]
        metadata_so = get_so(metadata_uuid, args.credentials, namespaces, "metadata")

        valid_all_top_level_so_conditions(top_level_so, pkg_type, args.collectionID)
        valid_all_contents_level_so_conditions(contents_so, pkg_type, args.collectionID)
        valid_all_metadata_level_so_conditions(metadata_so, pkg_type)

        # validate objects in contents folder, both IOs and SOs
        contents_io, contents_element_so = get_contents_io_so(
            contents_so, args.credentials, namespaces
        )
        contents_io_ct, contents_element_so_ct = get_contents_io_so_count(
            contents_io, contents_element_so
        )

        file_ct, folder_ct = get_source_file_folder_count(
            da_source, args.collectionID, top_level_so.title
        )
        valid_contents_count(contents_io_ct, contents_element_so_ct, file_ct, folder_ct)

        for io in contents_io:
            validate_all_contents_element_io_conditions(io, pkg_type)

        for so in contents_element_so:
            validate_all_contents_element_so_conditions(so, pkg_type)

        # validate objects in metadata folder, None or IOs
        if metadata_so.children:
            for child in metadata_so.children:
                metadata_io = get_io(
                    metadata_so.children[child]["uuid"], args.credentials, namespaces
                )
                validate_all_metadata_io_conditions(metadata_io)


if __name__ == "__main__":
    main()
