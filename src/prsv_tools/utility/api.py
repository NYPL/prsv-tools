import configparser
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests


def get_token(credential_file_name: str) -> str:
    """
    return token string
    check for existing valid token in token file
    if the file does not exist or the token is out of date, create token
    """

    token_file = Path(f"{credential_file_name}.token.file")
    if token_file.is_file():
        time_issued, sessiontoken = token_file.read_text().split("\n")
        # tokens are valid for 500 seconds
        if time.time() - float(time_issued) < 500:
            return sessiontoken

    return create_token(credential_file_name, token_file)


def create_token(credential_file_name: str, token_file: Path) -> str:
    """
    request token string based on credentials
    write time and token to a file and return token
    """

    config = configparser.ConfigParser()
    config.sections()
    credential_file = Path(credential_file_name)

    if credential_file.is_file():
        config.read(credential_file)
    else:
        raise FileNotFoundError

    # build the query string and get a new token
    url = config["DEFAULT"]["URL"]
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = (
        f'username={config["DEFAULT"]["Username"]}'
        f'&password={config["DEFAULT"]["Password"]}'
        f'&tenant={config["DEFAULT"]["Tenant"]}'
    )
    response = requests.request("POST", url, headers=headers, data=payload)
    data = response.json()

    # write token to token.file for later reuse
    token_file.write_text(f'{str(time.time())}\n{data["token"]}')

    return data["token"]


def find_apiversion(parsed_xml: ET.Element) -> str:
    version_search = re.search(r"v(\d+\.\d+)\}", parsed_xml.tag)
    if version_search:
        return version_search.group(1)
    else:
        return ""
