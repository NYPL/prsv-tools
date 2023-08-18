from pathlib import Path
import xml.etree.ElementTree as ET

import requests
import json

import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.cli as prsvcli

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
    query = {"q":"","fields":[{"name":"spec.specCollectionID","values":[collectionid]}]}
    q = json.dumps(query)
    url = f"https://nypl.preservica.com/api/content/search-within?q={q}&parenthierarchy={parentuuid}&start=0&max=-1&metadata=''" # noqa
    res = get_api_results(accesstoken, url)

    return res

def parse_structural_object_uuid(res):
    uuid_ls = list()
    json_obj = json.loads(res.text)
    obj_ids = json_obj["value"]["objectIds"]
    for sdbso in obj_ids:
        uuid_ls.append(sdbso[-36:])

    return uuid_ls

def ingest_has_correct_ER_number(collection_id, da_source, uuid_ls) -> bool:
    pkgs = [ x for x in da_source.iterdir() if x.is_dir() and x.name.startswith(collection_id)]
    expected = len(pkgs)
    found = len(uuid_ls)

    if expected == found:
        return True
    else:
        return False

def ingest_pkg_level_metadata_is_correct(uuid_ls, token) -> bool:
    for id in uuid_ls:
        url = f"https://nypl.preservica.com/api/entity/structural-objects/{id}"
        res = get_api_results(token, url)
        print(res.text)

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
        da_source = Path("/Users/hilaryszuyinshiue/mnt/preservica_da/data/Preservica_DigArch_Prod/DA_Source_Prod/DigArch")

    res_uuid = search_within_DigArch(token, args.collectionID, parentuuid)
    uuid_ls = parse_structural_object_uuid(res_uuid)
    # print(ingest_has_correct_ER_number(args.collectionID, da_source, uuid_ls))
    ingest_pkg_level_metadata_is_correct(uuid_ls, token)


if __name__ == "__main__":
    main()