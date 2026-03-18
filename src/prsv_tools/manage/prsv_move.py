# original prsv_move

import argparse
import logging
import json
import xml.etree.ElementTree as ET
from pathlib import Path
import datetime

import prsv_tools.utility.api as prsvapi
from prsv_tools.manage.create_pkg_report import search_preservica_api, requests_retry_session

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--credentials",
        type=str,
        required=True,
        help="which set of credentials to use"
    )
    parser.add_argument(
        "--pkgtitle",
        "-p",
        nargs='+',
        help="One or more titles of packages to find and move, separated by a space."
    )
    parser.add_argument(
        "--directory",
        help= "Path to a directory of packages to move.",
        type=Path
    )
    parser.add_argument(
        "--new-parent-ref",
        "-npf",
        required=True,
        help="The parentref of the new folder."
    )
    parser.add_argument(
        "--logpath", 
        type=Path,
        help="Directory for log files."
    )
    return parser.parse_args()


def fetch_uuid_by_title(accesstoken, title, parent_uuid, session):
    """Searches for an entity by title. Uses search-within if parent is provided, else standard search."""
    query_params = {
        "q": "",
        "fields": [{"name": "xip.title", "values": [title]}]
    }
    
    if parent_uuid:
        res = search_preservica_api(accesstoken, query_params, parent_uuid, session)
    else:
        base_url = "https://nypl.preservica.com/api/content/search"
        params = {
            'q': json.dumps(query_params),
            'start': 0,
            'max': 1,
            'metadata': "''"
        }
        headers = {"Preservica-Access-Token": accesstoken, "Accept": "application/json"}
        res = session.get(base_url, headers=headers, params=params)

    if res:
        try:
            json_obj = res.json()
            if json_obj.get("success") and json_obj.get("value", {}).get("totalHits") > 0:
                obj_id = json_obj["value"]["objectIds"][0]
                return obj_id[-36:]
        except (json.JSONDecodeError, IndexError, KeyError):
            pass
    return None

def move_entity(accesstoken, entity_uuid, new_parent_uuid, session):
    url = f"https://nypl.preservica.com/api/entity/structural-objects/{entity_uuid}/parent-ref"
    
    headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "text/plain",
        "accept": "text/plain;charset=UTF-8"
    }
    
    try:
        res = session.put(url, headers=headers, data=new_parent_uuid.strip())
        res.raise_for_status()
        
        return res.status_code == 202
        
    except Exception as e:
        logging.error(f"Error moving entity {entity_uuid}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response details: {e.response.text}")
        return False

def process_move_list(credentials: str, pkg_list: list, new_parent_ref: str, existing_logger=None):
    logger = existing_logger if existing_logger else logging.getLogger(__name__)
    
    session = requests_retry_session()

    ami_uuid = "183a74b5-7247-4fb2-8184-959366bc0cbc" if "test" not in credentials else ""
    digarch_uuid = "e80315bc-42f5-44da-807f-446f78621c08" if "test" not in credentials else "c0b9b47a-5552-4277-874e-092b3cc53af6"
    
    search_locations = {
        "DigAMI": ami_uuid,
        "DigArch": digarch_uuid,
        "Root": ""
    }

    failed_moves = set()
    successful_moves = set()
    deletion_exists = set()

    logger.info(f"Starting move workflow for {len(pkg_list)} packages.")

    for pkg_title in pkg_list:
        logger.info(f"--- Processing: {pkg_title} ---")
        
        accesstoken = prsvapi.get_token(credentials)
        
        if fetch_uuid_by_title(accesstoken, pkg_title, new_parent_ref, session):
            logger.info(f"SKIPPED: '{pkg_title}' already in destination.")
            deletion_exists.add(pkg_title)
            continue

        found_uuid = None
        for name, parent_uuid in search_locations.items():
            if parent_uuid is None: 
                continue 
            
            found_uuid = fetch_uuid_by_title(accesstoken, pkg_title, parent_uuid, session)
            if found_uuid:
                logger.info(f"Found '{pkg_title}' in {name} ({found_uuid})")
                break
        
        if found_uuid:
            success = move_entity(accesstoken, found_uuid, new_parent_ref, session)
            if success:
                logger.info(f"MOVED: '{pkg_title}' moved successfully.")
                successful_moves.add(pkg_title)
            else:
                logger.error(f"FAILED: Could not move '{pkg_title}'.")
                failed_moves.add(pkg_title)
        else:
            logger.warning(f"NOT FOUND: '{pkg_title}' could not be found in source folders.")
            failed_moves.add(pkg_title)

    logger.info("\n--- MOVE SUMMARY ---")
    logger.info(f"Successful: {len(successful_moves)}")
    logger.info(f"Already in Dest: {len(deletion_exists)}")
    logger.info(f"Failed/Not Found: {len(failed_moves)}")
    
    if failed_moves:
        for pkg in sorted(list(failed_moves)):
            logger.info(f"- {pkg}")

def main():
    args = parse_args()

    log_path = args.logpath if args.logpath else Path.cwd()
    log_file = log_path / f"prsv_move_{datetime.datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    if args.pkgtitle:
        process_move_list(
            credentials=args.credentials,
            pkg_list=args.pkgtitle,
            new_parent_ref=args.new_parent_ref,
            existing_logger=logger
        )
    elif args.directory:
        pkg_titles = [p.stem for p in args.directory.iterdir() if p.is_dir()]
        process_move_list(
            credentials=args.credentials,
            pkg_list=pkg_titles,
            new_parent_ref=args.new_parent_ref,
            existing_logger=logger
        )

if __name__ == "__main__":
    main()