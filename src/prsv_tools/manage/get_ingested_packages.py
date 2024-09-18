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
        "--dest",
        type=prsvcli.extant_dir,
        default=Path.cwd(),
        required=False,
        help="""Optional. Provide an absolute folder path to save the files
        in the specified folder""",
    )
    parser.add_argument(
        "--filter",
        type=str,
        required=False,
        help="""Optional. Provide filter to search for specific children""",
    )

    return parser.parse_args()


def get_api_results(accesstoken: str, url: str) -> requests.Response:
    headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "application/xml",
    }
    response = requests.request("GET", url, headers=headers)
    return response


def get_all_category_children(token: str, category_id: str, filter=None) -> list[str]:
    version = prsvapi.find_apiversion(token)
    ns = {"": f"{{http://preservica.com/EntityAPI/v{version}}}"}

    start = 0
    url = f"https://nypl.preservica.com/api/entity/structural-objects/86531e4f-3370-4944-9b70-6b64873226fa/children?start={start}&max=1"
    response = get_api_results(token, url)

    root = ET.fromstring(response.text)
    end = int(root.find(".//TotalResults", namespaces=ns).text)
    print(end)
    children = []
    while start < end:

        url = f"https://nypl.preservica.com/api/entity/structural-objects/86531e4f-3370-4944-9b70-6b64873226fa/children?start={start+1}&max=1000"
        response = get_api_results(token, url)
        root = ET.fromstring(response.text)
        children_results = root.findall(f".//Child", namespaces=ns)
        for child in children_results:
            if filter and child.get("title").startswith(filter):
                children_2 = [child.get("ref"), child.get("title")]
            else:
                children_2 = [child.get("ref"), child.get("title")]

            children.append(children_2)
        start += 1000

    return children


def get_all_category_grandchildren(token: str, children: list[str]) -> list[str]:
    version = prsvapi.find_apiversion(token)
    ns = {"": f"{{http://preservica.com/EntityAPI/v{version}}}"}
    good = []
    for child in children:
        url = f"https://nypl.preservica.com/api/entity/structural-objects/{child[0]}/children?start=1&max=2"
        response = get_api_results(token, url)
        if response.status_code != 200:
            token = prsvapi.get_token("prod-ingest")
            response = get_api_results(token, url)
        root = ET.fromstring(response.text)
        grandchild_maybe = root.find(".//Child", namespaces=ns)
        if grandchild_maybe is None:
            print(f"{child[1]} was a bad ingest?")
            continue
        grandchild = grandchild_maybe.get("ref")
        url2 = f"https://nypl.preservica.com/api/entity/structural-objects/{grandchild}/children"
        response2 = get_api_results(token, url2)
        if response2.status_code != 200:
            token = prsvapi.get_token("prod-ingest")
            response2 = get_api_results(token, url2)
        root2 = ET.fromstring(response2.text)
        total_ = root2.find(".//TotalResults", namespaces=ns)
        if total_ is None:
            print(f"{child[1]} was a bad ingest?")
            continue
        total = int(total_.text)

        if total == 0:
            print(f"{child[1]} was a bad ingest?")
        else:
            good.append(child)

    return good


def write_category_results(results: list[str], category: str, dest: Path) -> None:
    with open(dest.joinpath(f"{category}_children.txt"), "w") as f:
        for result in results:
            f.write(f"{result}\n")


def main():
    """
    Hard-coded variables include
        1. config files, which need to be in the same directory
        2. schemas_url, documents_url and transforms_url are relatively stable
    """

    args = parse_args()

    categories = {
        "DigAMI": "183a74b5-7247-4fb2-8184-959366bc0cbc",
        "DigArch": "e80315bc-42f5-44da-807f-446f78621c08",
        "BDAMI": "3e70b062-6e58-4dcd-84e8-e24194f3467d",
        "DigImages": "e544e461-3007-4de0-832d-381ec034424b",
    }

    token = prsvapi.get_token(args.credentials)

    # Fetch all children of parent
    results = get_all_category_children(token, categories["DigAMI"], args.filter)
    child_results = get_all_category_grandchildren(token, results)

    # Write all children to file
    fname = f"DigAMI_{args.filter}"
    write_category_results(child_results, fname, args.dest)


if __name__ == "__main__":
    main()
