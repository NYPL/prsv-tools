import logging
import xml.etree.ElementTree as ET

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


def retry_stalled_workflows(token: str) -> None:
    headers = {
        "Preservica-Access-Token": token,
        "Content-Type": "application/xml",
    }
    url = "https://nypl.preservica.com/sdb/rest/workflow/instances/retry?workflowInstanceIds="
    response = requests.request("POST", url, headers=headers)

    root = ET.fromstring(response.text)
    ns = {"": "http://workflow.preservica.com"}
    restart_count = root.find("SuccessfulNumber", namespaces=ns).text
    failed_count = root.find("FailedNumber", namespaces=ns).text
    LOGGER.info(
        f"Retried {restart_count + failed_count} worfklows: {restart_count} succeeded, {failed_count} failed"
    )

    return None


def main():
    args = parse_args()

    token = prsvapi.get_token(args.credentials)

    retry_stalled_workflows(token)

    return None


if __name__ == "__main__":
    main()
