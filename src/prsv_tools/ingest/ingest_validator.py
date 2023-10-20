import json
import logging
import re
import typing
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from pprint import pprint

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


def get_api_results(accesstoken: str, url: str) -> requests.Response:
    headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml",
    }
    response = requests.request("GET", url, headers=headers)
    return response


def search_within_DigArch(accesstoken, fields, parentuuid):
    query = {"q": "", "fields": fields}
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


def get_so(uuid, token, namespaces: dict, so_type: str):
    url = f"https://nypl.preservica.com/api/entity/structural-objects/{uuid}"
    res = get_api_results(token, url)
    root = ET.fromstring(res.text)

    uuid = root.find(f".//{namespaces['xip_ns']}Ref").text

    title = root.find(f".//{namespaces['xip_ns']}Title").text

    sectag = root.find(f".//{namespaces['xip_ns']}SecurityTag").text

    identifiers_url = root.find(f".//{namespaces['entity_ns']}Identifiers").text

    identifiers_res = get_api_results(token, identifiers_url)
    id_root = ET.fromstring(identifiers_res.text)

    type = id_root.find(f".//{namespaces['xip_ns']}Type").text
    soCat = id_root.find(f".//{namespaces['xip_ns']}Value").text

    if so_type in ["metadata", "contents_element"]:
        md = None
    else:
        metadata_url = root.find(f".//{namespaces['entity_ns']}Fragment").text

    if so_type == "top":
        md = get_spec_mdfrag(token, metadata_url, namespaces)
    elif so_type == "contents":
        md = get_fa_mdfrag(token, metadata_url, namespaces)

    children = get_so_children(token, uuid, namespaces)

    return prsv_Structural_Object(uuid, title, type, sectag, soCat, md, children)


def get_spec_mdfrag(token, metadata_url, namespaces: dict) -> dict:
    mdfrag_dict = dict()

    mfrag_res = get_api_results(token, metadata_url)
    mfrag_root = ET.fromstring(mfrag_res.text)

    speccolid_elem = mfrag_root.find(f".//{namespaces['spec_ns']}specCollectionId")
    mdfrag_dict["speccolID"] = speccolid_elem.text

    return mdfrag_dict


def get_fa_mdfrag(token, metadata_url, namespaces: dict) -> dict:
    mdfrag_dict = dict()

    mfrag_res = get_api_results(token, metadata_url)
    mfrag_root = ET.fromstring(mfrag_res.text)

    fa_component_id = mfrag_root.find(f".//{namespaces['fa_ns']}faComponentId")
    fa_collection_id = mfrag_root.find(f".//{namespaces['fa_ns']}faCollectionId")
    er_number = mfrag_root.find(f".//{namespaces['fa_ns']}erNumber")

    mdfrag_dict["faComponentId"] = fa_component_id.text
    mdfrag_dict["faCollectionId"] = fa_collection_id.text
    mdfrag_dict["erNumber"] = er_number.text

    return mdfrag_dict


def get_so_children(token, so_uuid, namespaces):
    children_dict = dict()

    def process_children_root(root):
        children = root.findall(f".//{namespaces['entity_ns']}Child")
        for child in children:
            title = child.attrib.get("title")
            ref = child.attrib.get("ref")
            type = child.attrib.get("type")
            children_dict[title] = {"objType": type, "uuid": ref}

    children_url = f"https://nypl.preservica.com/api/entity/structural-objects/{so_uuid}/children?start=0&max=1000"  # noqa

    children_res = get_api_results(token, children_url)
    children_root = ET.fromstring(children_res.text)
    process_children_root(children_root)

    if len(children_root.findall(f".//{namespaces['entity_ns']}Next")) > 0:
        next_children = children_root.findall(f".//{namespaces['entity_ns']}Next")
        for next_child in next_children:
            next_url = next_child.text
            next_res = get_api_results(token, next_url)
            next_root = ET.fromstring(next_res.text)
            process_children_root(next_root)

    return children_dict


def valid_top_so_title(top_level_so: dataclass):
    if re.fullmatch(r"M[0-9]+_(ER|DI|EM)_[0-9]+", top_level_so.title):
        return True
    else:
        logging.error(f"{top_level_so.title} does not confirm to convention")
        return False


def valid_contents_so_title(contents_so):
    if re.fullmatch(r"M[0-9]+_(ER|DI|EM)_[0-9]+_contents", contents_so.title):
        return True
    else:
        logging.error(f"{contents_so.title} does not confirm to convention")
        return False


def valid_open_sectag(so: dataclass):
    if so.securityTag == "open":
        return True
    else:
        logging.error(f"Security tag is not open, but {so.securityTag}")
        return False


def valid_so_type(so: dataclass):
    if so.type == "soCategory":
        return True
    else:
        logging.error(f"Type is not soCategory, but {so.type}")
        return False


def valid_top_so_category(top_level_so: dataclass):
    socat = re.search(r"[A-Z]{2}", top_level_so.title).group(0)

    if top_level_so.soCategory == f"{socat}Container":
        return True
    else:
        logging.error(
            f"{top_level_so.title} SO category is incorrect: {top_level_so.soCategory}"
        )
        return False


def valid_contents_so_category(contents_so: dataclass):
    socat = re.search(r"[A-Z]{2}", contents_so.title).group(0)

    if contents_so.soCategory == f"{socat}Contents":
        return True
    else:
        logging.error(
            f"{contents_so.title} SO category is incorrect: {contents_so.soCategory}"
        )
        return False


def valid_top_level_specId(top_level_so: dataclass, collectionId):
    if top_level_so.mdFragments["speccolID"] == collectionId:
        return True
    else:
        logging.error(
            f"Top SO Spec collection ID incorrect: {top_level_so.mdFragments['speccolID']}"
        )
        return False


def valid_contents_level_faComponentId(contents_so: dataclass):
    fa_component_id = re.search(
        r"(M[0-9]+_(ER|DI|EM)_[0-9]+)_contents", contents_so.title
    ).group(1)

    if contents_so.mdFragments["faComponentId"] == fa_component_id:
        return True
    else:
        logging.error(
            f"{contents_so.title} has incorrect faComponentId: {contents_so.mdFragments['faComponentId']}"
        )
        return False


def valid_contents_level_faCollectionId(contents_so: dataclass, collectionId):
    if contents_so.mdFragments["faCollectionId"] == collectionId:
        return True
    else:
        logging.error(
            f"{contents_so.title} has incorrect faCollectionId: {contents_so.mdFragments['faCollectionId']}"
        )
        return False


def valid_contents_level_erNumber(contents_so: dataclass):
    er_number = re.search(
        r"M[0-9]+_((ER|DI|EM)_[0-9]+)_contents", contents_so.title
    ).group(1)

    if contents_so.mdFragments["erNumber"] == er_number:
        return True
    else:
        logging.error(
            f"{contents_so.title} has incorrect erNumber: {contents_so.mdFragments['erNumber']}"
        )
        return False


def valid_all_top_level_so_conditions(top_level_so: dataclass, collectionId):
    logging.info(f"validating top level {top_level_so.title}")
    valid_top_so_title(top_level_so)
    valid_open_sectag(top_level_so)
    valid_so_type(top_level_so)
    valid_top_so_category(top_level_so)
    valid_top_level_specId(top_level_so, collectionId)


def valid_all_contents_level_so_conditions(contents_so: dataclass, collectionId):
    logging.info(f"validating contents level {contents_so.title}")
    valid_contents_so_title(contents_so)
    valid_open_sectag(contents_so)
    valid_so_type(contents_so)
    valid_contents_so_category(contents_so)
    valid_contents_level_faComponentId(contents_so)
    valid_contents_level_faCollectionId(contents_so, collectionId)
    valid_contents_level_erNumber(contents_so)


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
        "fa_ns": f"{{http://nypl.org/prsv_schemas/findingAid}}",
    }

    fields_top = [{"name": "spec.specCollectionID", "values": [args.collectionID]}]
    res_uuid = search_within_DigArch(token, fields_top, parentuuid)
    uuid_ls = parse_structural_object_uuid(res_uuid)

    ingest_has_correct_ER_number(args.collectionID, da_source, uuid_ls)

    for uuid in uuid_ls:
        top_level_so = get_so(uuid, token, namespaces, "top")
        # pprint(top_level_so)
        contents_f = f"{top_level_so.title}_contents"
        metadata_f = f"{top_level_so.title}_metadata"
        contents_uuid = top_level_so.children[contents_f]["uuid"]
        contents_so = get_so(contents_uuid, token, namespaces, "contents")
        # pprint(contents_so)
        metadata_uuid = top_level_so.children[metadata_f]["uuid"]
        metadata_so = get_so(metadata_uuid, token, namespaces, "metadata")
        # pprint(metadata_so)
        contents_children = get_so_children(token, contents_uuid, namespaces)
        # pprint(contents_children)

        valid_all_top_level_so_conditions(top_level_so, args.collectionID)
        valid_all_contents_level_so_conditions(contents_so, args.collectionID)


if __name__ == "__main__":
    main()
