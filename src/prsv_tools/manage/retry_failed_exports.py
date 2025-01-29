import logging
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.cli as prsvcli
import prsv_tools.manage.export_metadata_only as prsvem

logging.basicConfig(level=logging.INFO)

failed_pkg_id = list()

def parse_args():
    parser = prsvcli.Parser()

    parser.add_argument(
         "--credentials",
         type=str,
         required=True,
         choices=["prod-ingest"],
         help="which set of credentials to use",
    )


def failed_file_exists():
    """creates dict from files in failed exports path"""
    failed_pkg_id = list()
    failed_progress_token = list()
    failed_pkgs = dict()

    failed_path = Path("/containers/failed_metadata_exports")

    #create dict from pkg id and progress token in failed file name
    for file in failed_path.iterdir():
        failed_pkg_id.append(file.stem)
        failed_progress_token.append(file.suffix)
    
    for key in failed_pkg_id:
        for value in failed_progress_token:
            failed_pkgs[key] = value
    
    return failed_pkgs

def compare_downloads(failed_pkgs: dict):
    """compares pkg ids in failed pkgs dict to successfully exported files, returns true failed exports"""
    #pull in dict, separate into lists, compare against current downloads
    container_path = Path("/containers/metadata_exports")
    progress_token_path = Path(f"/containers/failed_metadata_exports/")
    failed_pkgs = failed_file_exists()
    
    for file in container_path.iterdir():
        for key, value in failed_pkgs:
            if key == file.stem:
                # if file is already downloaded, delete failed record
                failed_file = progress_token_path / f"{key}.{value}"
                failed_file.touch()
                failed_file.unlink(missing_ok=True)
                # remove not-failed item from dict
                failed_pkgs.pop(key)

    return failed_pkgs

def api_rerun(failed_pkgs: dict, accesstoken):
    """retruns the api call on the dict of true failed exports"""
    container_path = Path("/containers/metadata_exports/")
    for key, value in failed_pkgs:
        pkg_id = key
        progresstoken = value
        post_response = prsvem.post_so_api(pkg_id, accesstoken)

        pkg_dir_path = container_path / f"{pkg_id[:3]}"
        pkg_filepath = pkg_dir_path / f"{pkg_id}.zip"
        
        # check if top level pkg folder exists, create if not
        for file in container_path.iterdir():
            if file == pkg_id[:3]:
                continue
            else:
                pkg_dir_path.mkdir()
        #checking for status code
        if post_response.status_code == 202:
            logging.info(f"Now working on {pkg_id}") 
            logging.info(f"Progress token: {post_response.text}")
            progresstoken = post_response.text
            
            # create file recording pkg ID & progress token
            progress_token_path = Path(f"/containers/failed_metadata_exports/{pkg_id}.{progresstoken}")
            progress_token_path.touch()
            
            time.sleep(5)
        else:
            logging.error(
                f"POST request unsuccessful for {pkg_id}: code {post_response.status_code}"
            )
            sys.exit(0)

        for _ in range(5):
            time.sleep(15)
            get_progress_response = prsvem.get_progress_api(progresstoken, accesstoken)
            logging.info(get_progress_response.text)

            if get_progress_response.status_code != 200:
                logging.error(
                    f"""GET progress request unsuccessful for {pkg_id}:
                            code {get_progress_response.status_code}"""
                )
                return
            else:
                logging.info(f"Progress completed. Will proceed to download {pkg_id}")
                time.sleep(10)
                get_export_request = prsvem.get_export_download_api(
                    progresstoken, accesstoken
                )
                # checking for API status code
                if get_export_request.status_code == 200:
                    logging.info(
                        f"The exported content for {pkg_id} is in the process of being downloaded"
                    )
                    # save the file
                    save_file = open(pkg_filepath, "wb")
                    save_file.write(get_export_request.content)
                    save_file.close()
                    break
                else:
                    logging.error(
                        f"Get export request unsuccessful for {pkg_id}: {get_export_request.status_code}"
                    )

def main():
    args = parse_args()
    accesstoken = prsvapi.get_token(args.credentials)

    # failed files exist
    failed_files = failed_file_exists()
    # compared to downloaded files
    compared_files = compare_downloads(failed_files)
    # run parsed down api_status on true failed files
    api_rerun(compared_files, accesstoken)


if __name__ == "__main__":
    main()