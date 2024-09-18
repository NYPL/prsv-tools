import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET

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
        "--destination_folder_path",
        "-dest",
        type=prsvcli.extant_dir,
        required=False,
        help="""Optional. Provide an absolute folder path to save the files
        in the specified folder""",
    )
    parser.add_argument(
        "--collection_id",
        "-cid",
        required=False,
        help="""Provide collection ID, e.g. M1234, to export
        all collection packages metadata. Can take multiple ids, separate
        with space""",
    )
    parser.add_argument(
        "--package_id",
        "-pid",
        required=False,
        help="""Provide package ID, e.g. M1234_ER_1, to export
        the package's metadata. Can take multiple ids, separated with space""",
    )

    return parser.parse_args()


def parse_structural_object_uuid(res: requests.Response) -> list:
    """function to parse json API response into a list of UUIDs"""
    uuid_ls = list()
    json_obj = json.loads(res.text)
    obj_ids = json_obj["value"]["objectIds"]
    for sdbso in obj_ids:
        uuid_ls.append(sdbso[-36:])

    return uuid_ls


def search_preservica_api(
    accesstoken: str, query_params: dict, parentuuid: str
) -> requests.Response:
    query = json.dumps(query_params)
    search_url = f"https://nypl.preservica.com/api/content/search-within?q={query}&parenthierarchy={parentuuid}&start=0&max=-1&metadata=''"
    search_headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml;charset=UTF-8",
    }
    search_response = requests.get(search_url, headers=search_headers)
    return search_response


def get_collection_uuids(
    accesstoken: str, id: str, parentuuid: str
) -> requests.Response:
    query_params = {
        "q": "",
        "fields": [{"name": "spec.specCollectionID", "values": [id]}],
    }
    return search_preservica_api(accesstoken, query_params, parentuuid)


def get_packages_uuids(
    accesstoken: str, pkg_id: str, parentuuid: str
) -> requests.Response:
    col_id = re.search(r"(M\d+)_(ER|DI|EM)_\d+", pkg_id).group(1)
    query_params = {
        "q": "",
        "fields": [
            {"name": "xip.title", "values": [pkg_id]},
            {"name": "spec.specCollectionID", "values": [col_id]},
        ],
    }
    return search_preservica_api(accesstoken, query_params, parentuuid)


def get_pkg_title(accesstoken: str, pkg_uuid: str, credentials: str) -> str:
    get_so_url = f"https://nypl.preservica.com/api/entity/structural-objects/{pkg_uuid}"
    get_pkg_headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml;charset=UTF-8",
    }
    res = requests.get(get_so_url, headers=get_pkg_headers)

    root = ET.fromstring(res.text)
    version = prsvapi.find_apiversion(credentials)
    title = root.find(f".//{{http://preservica.com/XIP/v{version}}}Title").text

    return title


def post_so_api(uuid: str, accesstoken: str) -> requests.Response:
    """Make a POST request to the export Structural Object endpoint"""
    export_so_url = (
        f"https://nypl.preservica.com/api/entity/structural-objects/{uuid}/exports"
    )
    export_headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml;charset=UTF-8",
    }

    xml_str = (
        '<ExportAction xmlns="http://preservica.com/EntityAPI/v7.0" xmlns:xip="http://preservica.com/XIP/v7.0">'
        + "<IncludeContent>NoContent</IncludeContent>"
        + "<IncludeMetadata>Metadata</IncludeMetadata>"
        + "<IncludedGenerations>All</IncludedGenerations>"
        + "<IncludeParentHierarchy>false</IncludeParentHierarchy>"
        + "</ExportAction>"
    )
    # make the API call
    post_response = requests.post(export_so_url, headers=export_headers, data=xml_str)

    return post_response


def get_progress_api(progresstoken, accesstoken) -> requests.Response:
    """Make a GET request to check progress of the export request"""
    check_progress_url = f"https://nypl.preservica.com/api/entity/progress/{progresstoken}?includeErrors=true"

    get_progress_headers = {
        "Preservica-Access-Token": accesstoken,
        "accept": "application/xml;charset=UTF-8",
    }
    # make the API call
    get_progress_response = requests.get(
        check_progress_url, headers=get_progress_headers
    )

    return get_progress_response


def get_export_download_api(progresstoken, accesstoken):
    """Make a GET request to download the package"""
    get_export_url = f"https://nypl.preservica.com/api/entity/actions/exports/{progresstoken}/content"

    get_export_headers = {
        "Preservica-Access-Token": accesstoken,
        "accept": "application/octet-stream",
        "Content-Type": "application/xml;charset=UTF-8",
    }
    get_progress_response = requests.get(get_export_url, headers=get_export_headers)

    return get_progress_response


def main():
    args = parse_args()

    # generate token
    accesstoken = prsvapi.get_token(args.credentials)

    if "test" in args.credentials:
        digarch_uuid = "c0b9b47a-5552-4277-874e-092b3cc53af6"
    else:
        digarch_uuid = "e80315bc-42f5-44da-807f-446f78621c08"

    pkg_dict = dict()

    if args.collection_id:
        col_id_ls = args.collection_id.split()
        for col_id in col_id_ls:
            res = get_collection_uuids(accesstoken, col_id, digarch_uuid)
            so_uuids = parse_structural_object_uuid(res)

            for uuid in so_uuids:
                pkg_title = get_pkg_title(accesstoken, uuid, args.credentials)
                pkg_dict[pkg_title] = uuid
            print(pkg_dict)
    if args.package_id:
        pkg_id_ls = args.package_id.split()
        for pkg_id in pkg_id_ls:
            res = get_packages_uuids(accesstoken, pkg_id, digarch_uuid)
            uuid = parse_structural_object_uuid(res)
            for id in uuid:
                pkg_title = get_pkg_title(accesstoken, id, args.credentials)
                pkg_dict[pkg_title] = uuid[0]

    for pkg in pkg_dict:
        accesstoken_a = prsvapi.get_token(args.credentials)
        post_response = post_so_api(pkg_dict[pkg], accesstoken_a)

        # checking for API status code
        if post_response.status_code == 202:
            logging.info(f"Now working on {pkg}")
            logging.info(f"Progress token: {post_response.text}")
            progresstoken = post_response.text
            time.sleep(10)
        else:
            logging.error(
                f"POST request unsuccessful: code {post_response.status_code}"
            )
            sys.exit(0)

        # checking for API status code for 15 times. with 5 secs interval
        for _ in range(15):
            time.sleep(5)
            get_progress_response = get_progress_api(progresstoken, accesstoken_a)
            if get_progress_response.status_code != 200:
                logging.error(
                    f"""GET progress request unsuccessful:
                            code {get_progress_response.status_code}"""
                )
                return
            else:
                logging.info("Progress completed. Will proceed to download")
                time.sleep(60)
                get_export_request = get_export_download_api(
                    progresstoken, accesstoken_a
                )
                # checking for API status code
                if get_export_request.status_code == 200:
                    logging.info(
                        "The exported content is in the process of being downloaded"
                    )
                    # save the file
                    save_file = open(f"{pkg}.zip", "wb")  # wb: write binary
                    save_file.write(get_export_request.content)
                    save_file.close()
                    break
                else:
                    logging.error(
                        f"Get export request unsuccessful: {get_export_request.status_code}"
                    )


if __name__ == "__main__":
    main()
