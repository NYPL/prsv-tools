import requests
from pathlib import Path
import time
import logging
import sys

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

    return parser.parse_args()

def post_so_api(uuid: str, accesstoken: str) -> requests.Response:
    """Make a POST request to the export Structural Object endpoint"""
    export_so_url = f"https://nypl.preservica.com/api/entity/structural-objects/{uuid}/exports"
    export_headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml;charset=UTF-8",
    }

    xml_str = "<ExportAction xmlns=\"http://preservica.com/EntityAPI/v7.0\" xmlns:xip=\"http://preservica.com/XIP/v7.0\">" \
                                + "<IncludeContent>NoContent</IncludeContent>" \
                                + "<IncludeMetadata>Metadata</IncludeMetadata>" \
                                + "<IncludedGenerations>All</IncludedGenerations>" \
                                + "<IncludeParentHierarchy>true</IncludeParentHierarchy>" \
                                + "</ExportAction>"
    # make the API call
    post_response = requests.post(export_so_url, headers=export_headers, data=xml_str)

    return post_response

def get_progress_api(progresstoken, accesstoken) -> requests.Response:
    """Make a GET request to check progress of the export request"""
    check_progress_url = f"https://nypl.preservica.com/api/entity/progress/{progresstoken}?includeErrors=true"

    get_progress_headers = {
        "Preservica-Access-Token": accesstoken,
        "accept": "application/xml;charset=UTF-8"
    }
    # make the API call
    get_progress_response = requests.get(check_progress_url, headers=get_progress_headers)

    return get_progress_response

def get_export_download_api(progresstoken, accesstoken):
    """Make a GET request to download the package"""
    get_export_url = f"https://nypl.preservica.com/api/entity/actions/exports/{progresstoken}/content"

    get_export_headers = {
        "Preservica-Access-Token": accesstoken,
        "accept": "application/octet-stream",
        "Content-Type": "application/xml;charset=UTF-8"
        }
    get_progress_response = requests.get(get_export_url, headers=get_export_headers)

    return get_progress_response

def main():
    args = parse_args()

    # generate token
    accesstoken = prsvapi.get_token(args.credentials)
    so_uuid = "e80315bc-42f5-44da-807f-446f78621c08"

    post_response = post_so_api(so_uuid, accesstoken)

    # checking for API status code
    if post_response.status_code == 202:
        logging.info(f"Progress token: {post_response.text}")
        progresstoken = post_response.text
        time.sleep(10)
    else:
        logging.error(f"POST request unsuccessful: code {post_response.status_code}")
        sys.exit(0)

    # checking for API status code for 15 times. with 5 secs interval
    for _ in range(15):
        time.sleep(5)
        get_progress_response = get_progress_api(progresstoken, accesstoken)
        if get_progress_response.status_code != 200:
            logging.error(f"""GET progress request unsuccessful:
                          code {get_progress_response.status_code}""")
            return
        else:
            logging.info(f"Progress completed. Will proceed to download")
            time.sleep(60)
            get_export_request = get_export_download_api(progresstoken, accesstoken)
            # checking for API status code
            if get_export_request.status_code == 200:
                logging.info(f"The exported content is in the process of being downloaded")
                # save the file
                save_file = open(f"{so_uuid}.zip", "wb")  # wb: write binary
                save_file.write(get_export_request.content)
                save_file.close()
                break
            else:
                logging.error(f"Get export request unsuccessful: {get_export_request.status_code}")



if __name__ == "__main__":
    main()