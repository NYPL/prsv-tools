import json
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
import logging

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


def search_within_DigArch(accesstoken, collectionid, parentuuid):
    query = {
        "q": "",
        "fields": [{"name": "spec.specCollectionID", "values": [collectionid]}],
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

        url = f"https://nypl.preservica.com/api/entity/structural-objects/{parentuuid}"
        res = get_api_results(token, url)
        root = ET.fromstring(res.text)
        version = prsvapi.find_apiversion(root.tag)

        da_source = Path(
            "/Users/hilaryszuyinshiue/mnt/preservica_da/data/Preservica_DigArch_Prod/DA_Source_Prod/DigArch"
        )

    namespaces = {
        "xip_ns": f"{{http://preservica.com/XIP/v{version}}}",
        "entity_ns": f"{{http://preservica.com/EntityAPI/v{version}}}",
        "spec_ns": f"{{http://nypl.org/prsv_schemas/specCollection}}",
    }

    res_uuid = search_within_DigArch(token, args.collectionID, parentuuid)
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

        rm_list = ["id_url", "metadata_url", "children_url"]
        for key in rm_list:
            del so_dict[key]

        print(so_dict)


if __name__ == "__main__":
    main()
