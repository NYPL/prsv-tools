import xml.etree.ElementTree as ET
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
        "--destination_folder_path",
        "-dest",
        type=prsvcli.extant_dir,
        required=False,
        help="""Optional. Provide an absolute folder path to save the files
        in the specified folder""",
    )

    return parser.parse_args()


def get_api_results(accesstoken: str, url: str) -> requests.Response:
    headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml",
    }
    response = requests.request("GET", url, headers=headers)
    return response


def fetch_workflows(token, folder) -> None:
    url = f"https://nypl.preservica.com/sdb/rest/workflow/instances?state=active&type=Ingest&start=0&max=50&latestFirst=true&to={}" 
    response = get_api_results(token, url)
    root = ET.fromstring(response.text)
    ns = "http://workflow.preservica.com"
    hung_workflows = [instance for instance in root.iter(f"{ns}WorkflowInstance")]
    for workflow in hung_worfklows:
        # extract started, groupID, ContextName
        print(workflow.find("WorkflowContextName").text, workflow.find("Started").text, f"https://nypl.preservica.com/sdb/workflowProgress.html?wkId={workflow.find("WorkflowGroupID").text}", workflow.find("CurrentStepName").text))


def main():
    args = parse_args()

    if args.destination_folder_path:
        folder = Path(args.destination_folder_path)
    else:
        folder = Path.cwd()

    token = prsvapi.get_token(args.credentials)

    # Fetch and write schemas
    fetch_workflows(token, folder)


if __name__ == "__main__":
    main()
