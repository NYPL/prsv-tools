import xml.etree.ElementTree as ET
import logging
from pathlib import Path

import requests

import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.cli as prsvcli


LOGGER = logging.getLogger(__name__)


def parse_args():
    parser = prsvcli.Parser()

    parser.add_argument(
        "--credentials",
        type=str,
        required=True,
        choices=["test-ingest", "prod-ingest", "test-manage"],
        help="which set of credentials to use",
    )

    return parser.parse_args()


def get_api_results(accesstoken: str, url: str) -> requests.Response:
    headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml",
    }
    response = requests.request("GET", url, headers=headers)
    return response


def check_for_stalled_workflows(token: str) -> bool:
    response = get_api_results(token, "https://nypl.preservica.com/sdb/rest/workflow/instances?state=Failed&type=Ingest&start=0&max=100")
    root = ET.fromstring(response.text)
    ns = {"": "http://workflow.preservica.com"}
    if int(root.find(f"Count", namespaces=ns).text) > 0:
        hung_workflows = [instance for instance in root.iter(f"WorkflowInstance", namespaces=ns)]
        for workflow in hung_workflows:
            context = workflow.find("WorkflowContextName").text
            started = workflow.find("Started").text,
            step = workflow.find("CurrentStepName").text
            group_id = workflow.find("WorkflowGroupID").text
            LOGGER.info(f"Stalled workflow found: {context} started at {started} in group {group_id} at step {step}")


            LOGGER.info()
        return True
    else:
        return False


def retry_stalled_workflows(token: str) -> None:
    response = get_api_results(token, "https://nypl.preservica.com/sdb/rest/workflow/instances/retry")
    print(response.text)
    return None


def main():
    args = parse_args()

    token = prsvapi.get_token(args.credentials)

    # check for stalled workflows
    stalled = check_for_stalled_workflows(token)
    print(stalled)

    if stalled:
        print("Stalled workflows found")
        #retry_stalled_workflows(token, stalled)


if __name__ == "__main__":
    main()
