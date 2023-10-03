import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
import re

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
        help="the collection you'd like to check for, M\d+",
    )

    return parser.parse_args()


def get_api_results(accesstoken: str, url: str) -> requests.Response:
    headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml",
    }
    response = requests.request("GET", url, headers=headers)
    return response


def search_within_DigArch(accesstoken, fields, parentuuid):
    query = {
        "q": "",
        "fields": fields
    }
    q = json.dumps(query)
    url = f"https://nypl.preservica.com/api/content/search-within?q={q}&parenthierarchy={parentuuid}&start=0&max=-1&metadata=''"  # noqa
    res = get_api_results(accesstoken, url)

    return res


def parse_structural_object_uuid(res) -> list:
    uuid_ls = list()
    json_obj = json.loads(res.text)
    obj_ids = json_obj["value"]["objectIds"]
    for sdbso in obj_ids:
        uuid_ls.append(sdbso[-36:])

    return uuid_ls


def ingest_has_correct_ER_number(collection_id, da_source, uuid_ls) -> bool:
    pkgs = [
        x
        for x in da_source.iterdir()
        if x.is_dir() and x.name.startswith(collection_id)
    ]
    expected = len(pkgs)
    found = len(uuid_ls)

    if expected == found:
        logging.info(f"{collection_id} has correct number of packages in Preservica")
        return True
    else:
        logging.error(f"{collection_id} has incorrect number of packages in Preservica")
        return False


def get_so_metadata(uuid, token, namespaces: dict) -> dict:
    """return a dictionary with title, secturity tag (all strings)
    type, metadata fragments and children urls for other calls"""

    so_dict = dict()

    url = f"https://nypl.preservica.com/api/entity/structural-objects/{uuid}"
    res = get_api_results(token, url)
    root = ET.fromstring(res.text)

    title_elem = root.find(f".//{namespaces['xip_ns']}Title")
    so_dict["title"] = title_elem.text

    sectag_elem = root.find(f".//{namespaces['xip_ns']}SecurityTag")
    so_dict["sectag"] = sectag_elem.text

    identifiers_elem = root.find(f".//{namespaces['entity_ns']}Identifiers")
    so_dict["id_url"] = identifiers_elem.text

    metadata_elem = root.find(f".//{namespaces['entity_ns']}Fragment")
    so_dict["metadata_url"] = metadata_elem.text

    children_elem = root.find(f".//{namespaces['entity_ns']}Children")
    so_dict["children_url"] = children_elem.text

    return so_dict


def get_so_identifier(token, so_dict, namespaces: dict) -> dict:
    id_dict = dict()

    identifiers_res = get_api_results(token, so_dict["id_url"])
    id_root = ET.fromstring(identifiers_res.text)

    type_elem = id_root.find(f".//{namespaces['xip_ns']}Type")
    id_dict["type"] = type_elem.text

    value_elem = id_root.find(f".//{namespaces['xip_ns']}Value")
    id_dict["soCat"] = value_elem.text

    return id_dict


def get_spec_mdfrag(token, so_dict, namespaces: dict) -> dict:
    mdfrag_dict = dict()

    mfrag_res = get_api_results(token, so_dict["metadata_url"])
    mfrag_root = ET.fromstring(mfrag_res.text)

    speccolid_elem = mfrag_root.find(f".//{namespaces['spec_ns']}specCollectionId")
    mdfrag_dict["speccolID"] = speccolid_elem.text

    return mdfrag_dict


def get_fa_mdfrag(token, contents_so_dict, namespaces: dict) -> dict:
    mdfrag_dict = dict()

    mfrag_res = get_api_results(token, contents_so_dict["metadata_url"])
    mfrag_root = ET.fromstring(mfrag_res.text)

    fa_component_id = mfrag_root.find(f".//{namespaces['fa_ns']}faComponentId")
    fa_collection_id = mfrag_root.find(f".//{namespaces['fa_ns']}faCollectionId")
    er_number = mfrag_root.find(f".//{namespaces['fa_ns']}erNumber")

    mdfrag_dict["faComponentId"] = fa_component_id.text
    mdfrag_dict["faCollectionId"] = fa_collection_id.text
    mdfrag_dict["erNumber"] = er_number.text

    return mdfrag_dict

def get_so_children(token, so_dict, namespaces) -> dict:
    children_dict = dict()

    children_res = get_api_results(token, so_dict["children_url"])
    children_root = ET.fromstring(children_res.text)
    children = children_root.findall(f".//{namespaces['entity_ns']}Child")

    for c in children:
        if not children_dict:
            children_dict["children"] = []
        children_dict["children"].append(c.text)

    return children_dict

def validate_top_level_so(so_dict, collectionId):
    socat = re.search(r"[A-Z]{2}", so_dict["title"]).group(0)

    if not re.fullmatch(r"M[0-9]+_(ER|DI|EM)_[0-9]+", so_dict['title']):
        logging.error(f"Top level folder name incorrect {so_dict['title']}")
    elif not so_dict["sectag"] == "open":
        logging.error(f"Top level folder security tag incorrect: {so_dict['sectag']}")
    elif not so_dict["type"] == "soCategory":
        logging.error(f"Top level type is not soCategory")
    elif not so_dict["soCat"] == f"{socat}Container":
        logging.error(f"Top level SO Category is incorrect: {so_dict['soCat']}")
    elif not so_dict["speccolID"] == collectionId:
        logging.error(f"Top level SPEC Collection ID is incorrect: {so_dict['speccolID']}")
    else:
        logging.info(f"Top level folder {so_dict['title']} is VALID")

def validate_contents_so(contents_so_dict, collectionId):
    socat = re.search(r"[A-Z]{2}", contents_so_dict["title"]).group(0)
    fa_component_id = re.search(r"(M[0-9]+_(ER|DI|EM)_[0-9]+)_contents", contents_so_dict['title']).group(1)
    er_number = re.search(r"M[0-9]+_((ER|DI|EM)_[0-9]+)_contents", contents_so_dict['title']).group(1)

    if not re.fullmatch(r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents", contents_so_dict['title']):
        logging.error(f"Contents folder name incorrect {contents_so_dict['title']}")
    elif not contents_so_dict["sectag"] == "open":
        logging.error(f"Contents folder security tag incorrect: {contents_so_dict['sectag']}")
    elif not contents_so_dict["type"] == "soCategory":
        logging.error(f"Contents folder type is not soCategory")
    elif not contents_so_dict["soCat"] == f"{socat}Contents":
        logging.error(f"Contents SO Category is incorrect: {contents_so_dict['soCat']}")
    elif not contents_so_dict["faComponentId"] == fa_component_id:
        logging.error(f"Contents fa component ID is incorrect: {contents_so_dict['speccolID']}")
    elif not contents_so_dict["faCollectionId"] == collectionId:
        logging.error(f"Contents fa collection ID is incorrect: {contents_so_dict['faCollectionId']}")
    elif not contents_so_dict["erNumber"] == er_number:
        logging.error(f"Contents fa ER Number is incorrect: {contents_so_dict['erNumber']}")
    else:
        logging.info(f"Contents folder {contents_so_dict['title']} is VALID")



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

    token = prsvapi.get_token(args.credentials)
    if "test" in args.credentials:
        parentuuid = "c0b9b47a-5552-4277-874e-092b3cc53af6"
        da_source = Path("/data/Preservica_DigArch_Test/DA_Source_Test/DigArch")
    else:
        parentuuid = "e80315bc-42f5-44da-807f-446f78621c08"

        version = prsvapi.find_apiversion(token)

        da_source = Path(
            "/Users/hilaryszuyinshiue/mnt/vm/Preservica_DigArch_Prod/DA_Source_Prod/DigArch"
        )

    namespaces = {
        "xip_ns": f"{{http://preservica.com/XIP/v{version}}}",
        "entity_ns": f"{{http://preservica.com/EntityAPI/v{version}}}",
        "spec_ns": f"{{http://nypl.org/prsv_schemas/specCollection}}",
        "fa_ns": f"{{http://nypl.org/prsv_schemas/findingAid}}"
    }

    fields_top = [{"name": "spec.specCollectionID", "values": [args.collectionID]}]
    res_uuid = search_within_DigArch(token, fields_top, parentuuid)
    uuid_ls = parse_structural_object_uuid(res_uuid)

    ingest_has_correct_ER_number(args.collectionID, da_source, uuid_ls)

    for uuid in uuid_ls:
        so_dict = get_so_metadata(uuid, token, namespaces)

        id_dict = get_so_identifier(token, so_dict, namespaces)
        so_dict.update(id_dict)

        mdfrag_dict = get_spec_mdfrag(token, so_dict, namespaces)
        so_dict.update(mdfrag_dict)

        children_dict = get_so_children(token, so_dict, namespaces)
        so_dict.update(children_dict)

        for key in ["id_url", "metadata_url", "children_url"]:
            del so_dict[key]

        validate_top_level_so(so_dict, args.collectionID)

        contents_so_dict = get_so_metadata(so_dict["children"][0][-36:], token, namespaces)

        contents_id_dict = get_so_identifier(token, contents_so_dict, namespaces)
        contents_so_dict.update(contents_id_dict)

        contents_mdfrag_dict = get_fa_mdfrag(token, contents_so_dict, namespaces)
        contents_so_dict.update(contents_mdfrag_dict)

        contents_children_dict = get_so_children(token, contents_so_dict, namespaces)
        contents_so_dict.update(contents_children_dict)

        for key in ["id_url", "metadata_url", "children_url"]:
            del contents_so_dict[key]

        validate_contents_so(contents_so_dict, args.collectionID)

        """
        children of contents can be SO or IO
        if SO
        """



        # check second level SO M1234_ER_1_contents and M1234_ER_1_metadata
        # contents_title = f"{so_dict['title']}_contents"
        # fields_contents = [{"name": "xip.title", "values": [contents_title]}]
        # contents_so_uuid = search_within_DigArch(token, fields_contents, parentuuid)
        # contents_uuid_ls = parse_structural_object_uuid(contents_so_uuid)
        # print(contents_uuid_ls)








if __name__ == "__main__":
    main()
