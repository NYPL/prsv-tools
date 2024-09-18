import logging

import requests

import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.opex as prsvopex

LOGGER = logging.getLogger(__name__)

TOKEN = prsvapi.get_token("prod-ingest")
SEARCHURL = "https://nypl.preservica.com/api/content/search"
SOURL = "https://nypl.preservica.com/api/entity/structural-objects"

SESSION = requests.Session()
SESSION.headers = {
    "Preservica-Access-Token": TOKEN,
    "Content-Type": "application/x-www-form-urlencoded",
}


def get_response(url, data):
    return SESSION.post(url, data=data).json()


def search(
    coll_id: str = "", er_id: str = "", ami_id: str = ""
) -> list[prsvopex.Structural_Object]:
    if er_id == "all" or ami_id == "all":
        if not coll_id:
            raise ValueError("coll_id is blank")
        accepted = (er_id == "all", ami_id == "all")
        return search_coll(coll_id, accepted)
    if er_id:
        if not coll_id:
            raise ValueError("coll_id is blank")
        return search_item(
            {"findingAid.erNumber": er_id, "findingAid.faCollectionId": coll_id}
        )
    if ami_id:
        return search_item({"specObject.amiId": ami_id})


def search_item(ids) -> list[prsvopex.Structural_Object]:
    query = {"fields": []}
    for key, value in ids.items():
        query["fields"].append({"name": key, "values": value})

    metadata = [
        "xip.title",
        "xip.identifier",
        "xip.parent_ref",
        "xip.securitytag",
    ]

    data = {"q": query, "start": 0, "max": "1000", "metadata": metadata}

    response = get_response(SEARCHURL, data)
    total = response["value"]["totalHits"]

    if total == 0:
        raise ValueError("expected one result, got none")
    if total != 1:
        raise ValueError(f"expected one result, got {total}")

    so = create_object_from_search_response(
        response["value"]["objectIds"][0], response["value"]["metadata"][0]
    )
    return [so]


def search_coll(coll_id: str, accepted: list()) -> list[prsvopex.Structural_Object]:
    query = {"fields": [{"name": "spec.specCollectionID", "values": coll_id}]}

    metadata = [
        "xip.title",
        "xip.identifier",
        "xip.parent_ref",
        "xip.securitytag",
    ]

    data = {"q": query, "start": 0, "max": "1000", "metadata": metadata}

    response = get_response(SEARCHURL, data)

    total = response["value"]["totalHits"]
    if total == 0:
        raise ValueError("expected one result, got none")

    packages = []

    for i in range(0, response["value"]["totalHits"]):
        packages.append(
            create_object_from_search_response(
                response["value"]["objectIds"][i], response["value"]["metadata"][i]
            )
        )

    accepted_so = []
    if accepted[0]:
        accepted_so.extend(["ERContainer", "EMContainer", "DIContainer"])
    if accepted[1]:
        accepted_so.extend(["AMIContainer"])

    filtered_packages = [pkg for pkg in packages if pkg.soCategory in accepted_so]
    return filtered_packages


def create_object_from_search_response(
    uuid: str, md: dict
) -> prsvopex.Structural_Object:
    for field in md:
        match field["name"]:
            case "xip.title":
                title = field["value"]
            case "xip.securitytag":
                secTag = field["value"]
            case "xip.parent_ref":
                parent = field["value"]
            case "spec.specCollectionID":
                spec_coll_id = field["value"]
            case "xip.identifier":
                so_category = field["value"][0].split(" ")[1]

    so = prsvopex.Structural_Object(
        uuid=uuid.split("|")[1],
        title=title,
        securityTag=secTag,
        parent=parent,
        soCategory=so_category,
        mdFragments={"specCollId": spec_coll_id},
        children=None,
    )
    return so
