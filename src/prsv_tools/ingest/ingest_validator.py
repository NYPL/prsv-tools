from pathlib import Path

import requests

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

def search_within_DigArch(accesstoken):
    url = 'https://nypl.preservica.com/api/content/search-within?q={"q":"","fields":[{"name":"spec.specCollectionID","values":["M1126"]}]}&parenthierarchy=e80315bc-42f5-44da-807f-446f78621c08&start=0&max=-1&metadata=""' # noqa
    res = get_api_results(accesstoken, url)

    return res

def ingest_has_correct_ER_number(collection_id) -> bool:
    url = "https://nypl.preservica.com/api/content/search-within"

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
    response = search_within_DigArch(token)
    print(response.text)

if __name__ == "__main__":
    main()