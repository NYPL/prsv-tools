import argparse
import logging
import json
import xml.etree.ElementTree as ET
from pathlib import Path
import datetime
import requests
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.cli as prsvcli


def parse_args():
    parser = prsvcli.Parser(description="Update existing Preservica packages by moving JPEGs to a new _images IO and assigning identifiers.")
    parser.add_argument(
        "--credentials",
        type=str,
        required=True,
        help="Which set of credentials to use (e.g., 'test', 'prod').",
    )
    parser.add_argument(
        "--uuid",
        type=str,
        nargs='*',
        help="SO uuids to update. Accepts multiple values, ie. 1234 1242 4321, etc."
    )
    parser.add_argument(
        "--ami_id",
        "-ami",
        type=str,
        nargs='*',
        help="AMI ids to update. Accepts multiple values, ie. 1234 1242 4321, etc."
    )
    parser.add_argument(
        "--digarch_id",
        "-da",
        type=str,
        nargs='*',
        help="DigArch id to update. Accepts multiple values, ie. M1234_ER_1 M1242_ER_1, etc."
    )
    parser.add_argument(
        "--logpath", 
        type=Path,
        help="Directory for log files."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the changes without actually modifying data in Preservica."
    )
    return parser.parse_args()

def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def search_preservica_api(accesstoken: str, query_params: dict, parentuuid: str, session: requests.Session, logger) -> requests.Response:
    base_url = "https://nypl.preservica.com/api/content/search-within"
    params = {
        'q': json.dumps(query_params),
        'parenthierarchy': parentuuid,
        'start': 0,
        'max': -1,
        'metadata': "''" 
    }
    headers = {"Preservica-Access-Token": accesstoken, "Accept": "application/json"}
    
    try:
        res = session.get(base_url, headers=headers, params=params)
        res.raise_for_status()
        return res
    except requests.exceptions.RequestException as e:
        logger.error(f"API search request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
             logger.error(f"Response Body: {e.response.text}")
        return None

def get_single_ami_uuid(accesstoken: str, pkg_id: str, parentuuid: str, session: requests.Session, logger) -> str:
    query_params = {
        "q": "",
        "fields": [
            {"name": "xip.title", "values": [pkg_id]},
            {"name": "xip.identifier", "values": ["DigitizedAMIContainer"]}
        ]
    }
    res = search_preservica_api(accesstoken, query_params, parentuuid, session, logger)
    if res:
        try:
            json_obj = res.json()
            if json_obj.get("success") and json_obj.get("value", {}).get("totalHits") > 0:
                return json_obj["value"]["objectIds"][0][-36:]
        except (json.JSONDecodeError, IndexError, KeyError) as e:
            logger.error(f"Failed to parse JSON response or find uuid: {e}")
    logger.warning(f"No search results found for AMI id: {pkg_id}")
    return None

def get_digarch_uuids(accesstoken: str, pkg_id: str, parentuuid: str, session: requests.Session, logger) -> str:
    col_id_match = re.search(r"(M\d+)_(ER|DI|EM)_\d+", pkg_id)
    if not col_id_match:
        logger.error(f"Could not parse collection id from DigArch id: {pkg_id}")
        return None
    col_id = col_id_match.group(1)
    query_params = {
        "q": "",
        "fields": [
            {"name": "xip.title", "values": [pkg_id]},
            {"name": "spec.specCollectionID", "values": [col_id]},
        ],
    }
    response = search_preservica_api(accesstoken, query_params, parentuuid, session, logger)
    if response:
        try:
            json_obj = response.json()
            if json_obj.get("success") and json_obj.get("value", {}).get("totalHits") > 0:
                return json_obj["value"]["objectIds"][0][-36:]
        except (json.JSONDecodeError, IndexError, KeyError) as e:
            logger.error(f"Failed to parse JSON response or find uuid: {e}")
    logger.warning(f"No search results found for DigArch id: {pkg_id}")
    return None

def get_pkg_title(accesstoken: str, pkg_uuid: str, api_version: str, session: requests.Session, logger) -> str:
    url = f"https://nypl.preservica.com/api/entity/structural-objects/{pkg_uuid}"
    headers = {"Preservica-Access-Token": accesstoken, "accept": "application/xml;charset=UTF-8"}
    res = session.get(url, headers=headers)
    if res.status_code == 200:
        root = ET.fromstring(res.text)
        title_element = root.find(f".//{{http://preservica.com/XIP/v{api_version}}}Title")
        if title_element is not None and title_element.text:
            return title_element.text.strip()
    logger.error(f"Could not get title for {pkg_uuid}")
    return None


def fetch_entity_children(accesstoken, parent_uuid, session, credentials, logger, start=0, max=1000):
    """Gets direct children (SOs & IOs) of a Structural Object using the Entity API."""
    api_version = prsvapi.find_apiversion(credentials) or "6.0"
    url = f"https://nypl.preservica.com/api/entity/structural-objects/{parent_uuid}/children?start={start}&max={max}"
    headers = {"Preservica-Access-Token": accesstoken, "accept": "application/xml;charset=UTF-8"}
    res = session.get(url, headers=headers)
    children = []
    if res.status_code == 200:
        root = ET.fromstring(res.text)
        namespaces = {'entity': f'http://preservica.com/EntityAPI/v{api_version}'}
        for child in root.findall('.//entity:Child', namespaces):
            children.append({
                'ref': child.get('ref'),
                'type': child.get('type'),
                'title': child.get('title')
            })
    else:
        logger.error(f"Failed to fetch children for {parent_uuid}. API Status: {res.status_code}")
    return children

def create_information_object(title, parent_ref, accesstoken, session, credentials, logger, dry_run=False):
    if dry_run:
        logger.info(f"DRY RUN: Would create newIO '{title}' in parent {parent_ref}.")
        return f"DRY-RUN-UUID-{title}"
    api_version = prsvapi.find_apiversion(credentials) or "6.0"
    url = "https://nypl.preservica.com/api/entity/information-objects"
    headers = {"Preservica-Access-Token": accesstoken, "Content-Type": "application/xml"}
    xml_data = f"""<?xml version="1.0" encoding="UTF-8"?>
    <InformationObject xmlns="http://preservica.com/XIP/v{api_version}">
      <Title>{title}</Title>
      <SecurityTag>open</SecurityTag>
      <Parent>{parent_ref}</Parent>
    </InformationObject>"""
    res = session.post(url, headers=headers, data=xml_data)
    if res.status_code in (200, 201):
        root = ET.fromstring(res.text)
        for elem in root.iter():
            if elem.tag.endswith('Ref'):
                return elem.text
    return None

def move_entity(accesstoken, entity_uuid, new_parent_uuid, session, logger, entity_type="information-objects", dry_run=False):
    if dry_run:
        logger.info(f"DRY RUN: Would move {entity_uuid} to parent {new_parent_uuid}.")
        return True
    url = f"https://nypl.preservica.com/api/entity/{entity_type}/{entity_uuid}/parent-ref"
    headers = {"Preservica-Access-Token": accesstoken, "Content-Type": "text/plain", "accept": "text/plain;charset=UTF-8"}
    res = session.put(url, headers=headers, data=new_parent_uuid.strip())
    return res.status_code == 202

def update_identifier(ref, new_category_value, accesstoken, session, logger, dry_run=False):
    url = f"https://nypl.preservica.com/api/entity/information-objects/{ref}/identifiers"
    headers = {"Preservica-Access-Token": accesstoken, "accept": "application/xml;charset=UTF-8", "Content-Type": "application/xml;charset=UTF-8"}
    
    res_get = session.get(url, headers={"Preservica-Access-Token": accesstoken, "accept": "application/xml;charset=UTF-8"})
    if res_get.status_code == 200:
        root = ET.fromstring(res_get.text)
        for identifier in root.iter():
            if identifier.tag.endswith('Identifier'):
                type_elem = val_elem = None
                for child in identifier:
                    if child.tag.endswith('Type'): type_elem = child
                    elif child.tag.endswith('Value'): val_elem = child
                if type_elem is not None and type_elem.text == "ioCategory":
                    if val_elem is not None and val_elem.text == new_category_value:
                        logger.info(f"Identifier ioCategory={new_category_value} already exists on {ref}. Skipping.")
                        return True
                    else:
                        logger.warning(f"Different ioCategory ({val_elem.text}) exists on {ref}. Adding new one may cause conflicts.")
    else:
        if dry_run and "DRY-RUN-UUID" in ref: pass 
        else:
            logger.error(f"Failed to get existing identifiers for {ref}.")
            return False
            
    if dry_run:
        logger.info(f"DRY RUN: Would add identifier ioCategory={new_category_value} on {ref}.")
        return True

    post_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <Identifier>
        <Type>ioCategory</Type>
        <Value>{new_category_value}</Value>
        <Entity>{ref}</Entity>
    </Identifier>"""
    
    res_post = session.post(url, headers=headers, data=post_xml)
    if res_post.status_code not in (200, 201, 202):
        logger.error(f"Failed to post identifier {new_category_value} for {ref}. Status: {res_post.status_code}")
        return False
        
    res_confirm = session.get(url, headers={"Preservica-Access-Token": accesstoken, "accept": "application/xml;charset=UTF-8"})
    confirm_root = ET.fromstring(res_confirm.text)
    
    success = False
    for identifier in confirm_root.iter():
        if identifier.tag.endswith('Identifier'):
            type_elem = val_elem = None
            for child in identifier:
                if child.tag.endswith('Type'): type_elem = child
                elif child.tag.endswith('Value'): val_elem = child
            if type_elem is not None and type_elem.text == "ioCategory" and val_elem is not None and val_elem.text == new_category_value:
                success = True
                break
            
    if success: logger.info(f"Successfully added and confirmed identifier ioCategory={new_category_value} on {ref}.")
    else: logger.error(f"POST succeeded but confirmation GET failed to find ioCategory={new_category_value} on {ref}.")
    return success

def process_package(pkg_uuid, pkgtitle, accesstoken, credentials, session, logger, is_dry_run):

    logger.info(f"Retrieving children for package ({pkg_uuid})...")
    pkg_children = fetch_entity_children(accesstoken, pkg_uuid, session, credentials, logger)
    
    metadata_uuid = media_uuid = contents_uuid = None
    for child in pkg_children:
        title = child.get('title', '')
        if title == f"{pkgtitle}_metadata": metadata_uuid = child['ref']
        elif title == f"{pkgtitle}_media": media_uuid = child['ref']
        elif title == f"{pkgtitle}_contents": contents_uuid = child['ref']
            
    if not contents_uuid:
        logger.error(f"Could not find '{pkgtitle}_contents'. Cannot create _images destination.")
        return
        
    logger.info(f"Retrieving children of '{pkgtitle}_contents' to locate _images.")
    contents_children = fetch_entity_children(accesstoken, contents_uuid, session, credentials, logger)
    images_title = f"{pkgtitle}_images"
    images_uuid = None
    
    for child in contents_children:
        if child.get('title') == images_title:
            images_uuid = child['ref']
            break
    
    if not images_uuid:
        logger.info(f"Information Object '{images_title}' not found, creating.")
        images_uuid = create_information_object(images_title, contents_uuid, accesstoken, session, credentials, logger, dry_run=is_dry_run)
        if not images_uuid:
            logger.error(f"Failed to create {images_title}.")
            return
            
    update_identifier(images_uuid, "AMIImage", accesstoken, session, logger, dry_run=is_dry_run)

    source_folders = []
    if metadata_uuid: source_folders.append(("_metadata", metadata_uuid))
    if media_uuid: source_folders.append(("_media", media_uuid))
    
    jpegs_to_process = []
    for folder_name, folder_uuid in source_folders:
        logger.info(f"Scanning {folder_name} ({folder_uuid}) for JPEGs...")
        children = fetch_entity_children(accesstoken, folder_uuid, session, credentials, logger)
        for child in children:
            if child.get('title', '').lower().endswith(('.jpg', '.jpeg')):
                jpegs_to_process.append(child)

    if not jpegs_to_process:
        logger.info("No JPEGs found in _metadata or _media.")
        return

    logger.info(f"Found {len(jpegs_to_process)} JPEGs. Preparing to move to {images_title} ({images_uuid}).")
    
    for jpeg in jpegs_to_process:
        title = jpeg['title']
        ref = jpeg['ref']
        if move_entity(accesstoken, ref, images_uuid, session, logger, entity_type="information-objects", dry_run=is_dry_run):
            if not is_dry_run: logger.info(f"Moved '{title}' successfully.")
            category_val = "Carrierphotograph" if any(keyword in title for keyword in ["AssetFront", "AssetBack", "AssetSide"]) else "AMIImage"
            update_identifier(ref, category_val, accesstoken, session, logger, dry_run=is_dry_run)
        else:
            logger.error(f"Failed to move '{title}'. Identifier update skipped.")

def main():
    args = parse_args()
    log_path = args.logpath if args.logpath else Path.cwd()
    log_prefix = "DRY_RUN_" if args.dry_run else ""
    log_file = log_path / f"{log_prefix}update_jpeg_metadata_{datetime.datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler()])
    logger = logging.getLogger(__name__)

    try:
        accesstoken = prsvapi.get_token(args.credentials)
        session = requests_retry_session()
        api_version = prsvapi.find_apiversion(args.credentials) or "6.0"

        if "test" in args.credentials:
            digarch_parent_uuid = "c0b9b47a-5552-4277-874e-092b3cc53af6"
            ami_parent_uuid = ""
        else:
            digarch_parent_uuid = "e80315bc-42f5-44da-807f-446f78621c08"
            ami_parent_uuid = "183a74b5-7247-4fb2-8184-959366bc0cbc"

        identifiers = []
        if args.uuid:
            identifiers.extend(args.uuid)
        if args.ami_id:
            for id in args.ami_id:
                uuid = get_single_ami_uuid(accesstoken, id, ami_parent_uuid, session, logger)
                if uuid: identifiers.append(uuid)
        if args.digarch_id:
            for digarch_id in args.digarch_id:
                uuid = get_digarch_uuids(accesstoken, digarch_id, digarch_parent_uuid, session, logger)
                if uuid: identifiers.append(uuid)

        if not identifiers:
            logger.error("No valid uuids were found.")
            return

        if args.dry_run:
            logger.info("DRY RUN ENABLED - NO CHANGES WILL BE MADE")

        # Process Each Found Package
        for pkg_uuid in identifiers:
            pkgtitle = get_pkg_title(accesstoken, pkg_uuid, api_version, session, logger)
            if pkgtitle:
                process_package(pkg_uuid, pkgtitle, accesstoken, args.credentials, session, logger, args.dry_run)
            else:
                logger.error(f"Skipping UUID {pkg_uuid} because title could not be resolved.")

    except Exception as e:
        logger.exception(f"Workflow failed: {str(e)}")

if __name__ == "__main__":
    main()