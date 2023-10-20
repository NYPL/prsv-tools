import json
import logging
import xml.etree.ElementTree as ET

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


def search(coll_id: str) -> list[prsvopex.Structural_Object]:
    filter = {"fields": []}
    if coll_id:
        filter["fields"].append({"name": "spec.specCollectionID", "values": coll_id})

    metadata = [
        "xip.title",
        "xip.identifier",
        "xip.parent_ref",
        "xip.securitytag",
        "spec.specCollectionID",
    ]
    data = {"q": filter, "start": 0, "max": "1000", "metadata": metadata}

    response = get_response(SEARCHURL, data)

    packages = []

    for i in range(0, len(response["value"]["objectIds"])):
        response["value"]["objectIds"]
        md = response["value"]["metadata"][i]
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
        packages.append(
            prsvopex.Structural_Object(
                uuid=response["value"]["objectIds"][i].split("|")[1],
                title=title,
                securityTag=secTag,
                parent=parent,
                soCategory=so_category,
                mdFragments={"specCollId": spec_coll_id},
                children=None,
            )
        )

    return packages
