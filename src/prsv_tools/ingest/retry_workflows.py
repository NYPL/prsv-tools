import json
import logging
import sys
import xml.etree.ElementTree as ET

import requests

import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.cli as prsvcli

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    LOGGER.addHandler(logging.StreamHandler())
    LOGGER.setLevel(logging.INFO)


def parse_args() -> prsvcli.argparse.Namespace:
    parser = prsvcli.Parser()

    parser.add_argument(
        "--credentials",
        type=str,
        required=True,
        choices=["test-ingest", "prod-ingest", "test-manage"],
        help="which set of credentials to use",
    )

    return parser.parse_args()


API_PATH_PREFIX = "/sdb/rest/workflow"

def find_workflows_to_resume(base_url: str, token: str) -> list[str]:
    """
    Finds all active ingest workflows at the 'DeleteIngestedFromSource' step.

    Handles pagination to retrieve all results.
    """

    workflow_ids: list[str] = []
    start: int = 0
    max_results: int = 20

    find_url = f"{base_url}{API_PATH_PREFIX}/instances"

    headers = {
        "Preservica-Access-Token": token,
        "accept": "application/xml",
        "charset": "UTF-8"
    }

    # Parameters to filter the search
    params = {
        "state": "Active",
        "type": "Ingest"
    }

    #print(f"[*] Searching for workflows at '{params['activeStepName']}' step...")

    while True:
        try:
            # Add pagination parameters
            params["start"] = start
            params["max"] = max_results

            response = requests.get(find_url, headers=headers, params=params, timeout=30)

            # Check for auth errors or other issues
            response.raise_for_status()

            root = ET.fromstring(response.text)
            ns = {"": "http://workflow.preservica.com"}

            instances = root.findall("WorkflowInstance", namespaces=ns)
            if not len(instances):
                # No more results, we're done
                break

            # Collect the IDs
            for instance in instances:
                if instance.find('./CurrentStepName', ns).text == "DeleteIngestedFromSource":
                    id_node = instance.find('./Id', ns) # Find the 'Id' child tag
                    if id_node is not None:
                        workflow_ids.append(id_node.text)

            # Move to the next page
            start += max_results

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("\n[!] Authentication Failed: Invalid Preservica-Access-Token.", file=sys.stderr)
            else:
                print(f"\n[!] HTTP Error: {e}", file=sys.stderr)
            break
        except requests.exceptions.RequestException as e:
            print(f"\n[!] Network Error: {e}", file=sys.stderr)
            break
        except json.JSONDecodeError:
            print("\n[!] Failed to decode JSON response from server.", file=sys.stderr)
            break

    return workflow_ids


def resume_workflows(base_url: str, token: str, workflow_ids: list[str]):
    """
    Attempts to resume a list of workflows by their IDs.
    """

    headers = {
        "Preservica-Access-Token": token,
        "accept": "application/xml",
        "charset": "UTF-8"
    }

    action = "retry"
    print(f"[*] Attempting to {action} {len(workflow_ids)} workflows...")

    for workflow_id in workflow_ids:
        resume_url = f"{base_url}{API_PATH_PREFIX}/instances/{action}"

        try:
            response = requests.post(resume_url, headers=headers, params={"workflowInstanceIds": workflow_id, "state": "Running"},timeout=30)
            print(response.url)

            if response.status_code == 200:
                root = ET.fromstring(response.text)
                ns = {"": "http://workflow.preservica.com"}
                if root.find('./SuccessfulNumber', ns).text == "1":
                    print(f'Workflow {workflow_id} {action} request successful.')
                else:
                    print(f'Workflow {workflow_id} {action} request failed.')
            else:
                print(f'Workflow {workflow_id} {action} request failed with status code {response.status_code}.')

        except requests.exceptions.RequestException as e:
            print(f'Workflow {workflow_id} {action} request failed with network error: {e}')

    return


def main() -> None:
    configure_logging()

    args = parse_args()

    token = prsvapi.get_token(args.credentials)

    stuck_results = find_workflows_to_resume(
        base_url="https://nypl.preservica.com",
        token=token
    )
    print(stuck_results)
    resume_workflows(
        base_url="https://nypl.preservica.com",
        token=token,
        workflow_ids=stuck_results
    )

    return None


if __name__ == "__main__":
    main()
