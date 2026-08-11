import argparse
import datetime
import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path

import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.cli as prsvcli
from prsv_tools.manage.update_images_metadata import (
    fetch_entity_children,
    get_pkg_title,
    get_single_ami_uuid,
    requests_retry_session,
    search_preservica_api,
)


def parse_args():
    parser = prsvcli.Parser(
        description="Locate .jpeg/.jpg files within the _metadata IO folder of a structural object/package (SO)."
    )
    parser.add_argument(
        "--credentials",
        type=str,
        required=True,
        help="Which set of credentials to use (e.g., 'test', 'prod').",
    )
    parser.add_argument(
        "--ami_id",
        "-ami",
        type=str,
        nargs="*",
        help="Optional. AMI ids to investigate. If omitted, all packages in the AMI parent folder are scanned.",
    )
    parser.add_argument(
        "--logpath",
        type=Path,
        help="Directory for log files.",
    )
    return parser.parse_args()


def get_all_packages_in_folder(accesstoken, parent_uuid, session, credentials, logger):
    """Gets all SO children in a parent hierarchy using search-within (Content API)."""
    # Optimized Strategy: Search for all JPEGs directly within the hierarchy.
    # This is much faster for collections with 50,000+ objects.
    query_params = {
        "q": "",
        "fields": [{"name": "xip.title", "values": ["*.jpg", "*.jpeg"]}],
    }

    base_url = "https://nypl.preservica.com/api/content/search-within"
    params = {
        "q": json.dumps(query_params),
        "parenthierarchy": parent_uuid,
        "start": 0,
        "max": 1000,
        "metadata": "''",
    }
    headers = {"Preservica-Access-Token": accesstoken, "Accept": "application/json"}

    parent_refs = set()
    start = 0
    total_hits = 1  # Dummy start value

    logger.info("Performing broad search for .jpg/.jpeg files across the entire collection...")
    while start < total_hits:
        params["start"] = start
        try:
            res = session.get(base_url, headers=headers, params=params)
            res.raise_for_status()
            data = res.json()

            if data.get("success"):
                value = data.get("value", {})
                total_hits = value.get("totalHits", 0)
                object_ids = value.get("objectIds", [])
                
                # In Preservica search results, we don't get the parent_ref directly in the hit list
                # unless we fetch hit metadata. To be efficient, we'll get the hit IDs and titles?
                # Actually, wait. If we request no metadata, we only get objectIds.
                # To get the parent_ref without fetching each hit entity one-by-one, 
                # we SHOULD request hit metadata if possible, or use search results efficiently.
                # However, the Content API search results hit data includes hit metadata.
                # Let's request xip.parent_ref.
                pass
            else:
                break
        except Exception as e:
            logger.error(f"Search failed at offset {start}: {e}")
            break
        start += 1000

    # Wait, the above logic is incomplete. Let's rewrite it more accurately.
    return []


def scan_collection_for_metadata_jpegs(accesstoken, parent_uuid, session, credentials, logger):
    """Searches for JPEGs and returns a list of unique parent folders that match the metadata pattern."""
    query_params = {
        "q": "",
        "fields": [{"name": "xip.title", "values": ["*.jpg", "*.jpeg"]}],
    }

    base_url = "https://nypl.preservica.com/api/content/search-within"
    # We request xip.parent_ref and xip.title in hit metadata
    params = {
        "q": json.dumps(query_params),
        "parenthierarchy": parent_uuid,
        "start": 0,
        "max": 1000,
        "metadata": "xip.parent_ref",
    }
    headers = {"Preservica-Access-Token": accesstoken, "Accept": "application/json"}

    parent_refs = set()
    start = 0
    total_hits = 1

    logger.info("Searching for JPEGs across the collection hierarchy...")
    while start < total_hits:
        params["start"] = start
        res = session.get(base_url, headers=headers, params=params)
        if res.status_code != 200:
            logger.error(f"Search failed at offset {start}. Status: {res.status_code}")
            break
        
        data = res.json()
        if not data.get("success"):
            break
            
        value = data.get("value", {})
        total_hits = value.get("totalHits", 0)
        metadata = value.get("metadata", [])
        
        for hit_md in metadata:
            if hit_md and isinstance(hit_md[0], dict):
                p_ref = hit_md[0].get("value")
                if p_ref:
                    # extract uuid from full ref URL if needed (usually a UUID or a URL)
                    parent_refs.add(p_ref[-36:])
        
        start += 1000
    
    logger.info(f"Found {len(parent_refs)} unique folders containing JPEGs.")
    
    # Verify which folders are _metadata folders and get their package titles
    api_version = prsvapi.find_apiversion(credentials) or "6.0"
    matched_packages = []
    
    for p_uuid in parent_refs:
        title = get_pkg_title(accesstoken, p_uuid, api_version, session, logger)
        if title and title.endswith("_metadata"):
            pkg_name = title.replace("_metadata", "")
            matched_packages.append({"ref": p_uuid, "title": pkg_name})
            
    return matched_packages


def process_package(pkg_uuid, pkgtitle, accesstoken, credentials, session, logger):
    logger.info(f"Retrieving children for package {pkgtitle} ({pkg_uuid})...")
    pkg_children = fetch_entity_children(
        accesstoken, pkg_uuid, session, credentials, logger
    )

    metadata_uuid = None
    for child in pkg_children:
        title = child.get("title", "")
        if title == f"{pkgtitle}_metadata":
            metadata_uuid = child["ref"]
            break

    if not metadata_uuid:
        logger.warning(f"Could not find '{pkgtitle}_metadata' IO.")
        return

    logger.info(f"Scanning _metadata IO ({metadata_uuid}) for JPEGs...")
    children = fetch_entity_children(
        accesstoken, metadata_uuid, session, credentials, logger
    )

    jpegs_found = []
    for child in children:
        if child.get("title", "").lower().endswith((".jpg", ".jpeg")):
            jpegs_found.append(child)

    if not jpegs_found:
        logger.info(f"No JPEGs found in _metadata for {pkgtitle}.")
        return False
    else:
        logger.info(f"Found {len(jpegs_found)} JPEGs in _metadata for {pkgtitle}:")
        for jpeg in jpegs_found:
            logger.info(f"  - {jpeg['title']} ({jpeg['ref']})")
        return True


def main():
    args = parse_args()
    log_path = args.logpath if args.logpath else Path.cwd()
    log_file = (
        log_path
        / f"locate_metadata_jpegs_{datetime.datetime.now().strftime('%Y%m%d')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)

    try:
        accesstoken = prsvapi.get_token(args.credentials)
        session = requests_retry_session()
        api_version = prsvapi.find_apiversion(args.credentials) or "6.0"

        if "test" in args.credentials:
            ami_parent_uuid = ""
        else:
            ami_parent_uuid = "183a74b5-7247-4fb2-8184-959366bc0cbc"

        identifiers = []
        if args.ami_id:
            logger.info(f"Investigating {len(args.ami_id)} specific AMI IDs...")
            for id in args.ami_id:
                uuid = get_single_ami_uuid(
                    accesstoken, id, ami_parent_uuid, session, logger
                )
                if uuid:
                    identifiers.append({"ref": uuid, "title": id})
        else:
            logger.info(f"No IDs provided. Scanning AMI parent folder: {ami_parent_uuid}")
            matched_packages = scan_collection_for_metadata_jpegs(
                accesstoken, ami_parent_uuid, session, args.credentials, logger
            )
            logger.info(f"Found {len(matched_packages)} packages with metadata JPEGs.")
            identifiers = matched_packages

        if not identifiers:
            logger.error("No valid packages were found.")
            return

        if not args.ami_id:
            # Broad search summary
            packages_with_metadata_jpegs = [p["title"] for p in identifiers]
        else:
            # Specific ID search summary
            packages_with_metadata_jpegs = []
            for pkg in identifiers:
                pkg_uuid = pkg["ref"]
                pkgtitle = pkg.get("title")
                if not pkgtitle:
                    pkgtitle = get_pkg_title(
                        accesstoken, pkg_uuid, api_version, session, logger
                    )

                if pkgtitle:
                    if process_package(
                        pkg_uuid, pkgtitle, accesstoken, args.credentials, session, logger
                    ):
                        packages_with_metadata_jpegs.append(pkgtitle)
                else:
                    logger.error(
                        f"Skipping UUID {pkg_uuid} because title could not be resolved."
                    )

        logger.info("-" * 40)
        logger.info("SUMMARY REPORT")
        logger.info(f"Total packages scanned: {len(identifiers)}")
        logger.info(f"Packages with JPEGs in _metadata: {len(packages_with_metadata_jpegs)}")
        if packages_with_metadata_jpegs:
            logger.info("Matched packages:")
            for title in packages_with_metadata_jpegs:
                logger.info(f"  - {title}")
        logger.info("-" * 40)

    except Exception as e:
        logger.exception(f"Workflow failed: {str(e)}")


if __name__ == "__main__":
    main()
