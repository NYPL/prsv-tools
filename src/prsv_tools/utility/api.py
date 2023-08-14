import configparser
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

import prsv_tools.utility.creds as prsvcreds

TOKEN_BASE_URL = "https://nypl.preservica.com/api/accesstoken/login"


def get_token(credential_set: str) -> str:
    """
    return token string
    check for existing valid token in token file
    if the file does not exist or the token is out of date, create token
    """

    token_file = Path(f"{credential_set}.token.file")
    if token_file.is_file():
        time_issued, sessiontoken = token_file.read_text().split("\n")
        # tokens are valid for 500 seconds
        if time.time() - float(time_issued) < 500:
            return sessiontoken

    return create_token(credential_set, token_file)


def create_token(credential_set: str, token_file: Path) -> str:
    """
    request token string based on credentials
    write time and token to a file and return token
    """

    creds = prsvcreds.Credentials()
    user, pw, tenant = creds.get_credentials(credential_set)

    # build the query string and get a new token
    url = TOKEN_BASE_URL
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = f"username={user}&password={pw}&tenant={tenant}"
    response = requests.post(url, headers=headers, data=payload)
    data = response.json()

    if not data["success"]:
        raise prsvcreds.PrsvCredentialException(
            f"Invalid credentials. Update the file at {str(prsvcreds.CREDS_INI)}"
        )

    # write token to token.file for later reuse
    token_file.write_text(f'{str(time.time())}\n{data["token"]}')

    return data["token"]


def find_apiversion(xml_root_tag: str) -> str:
    version_search = re.search(r"v(\d+\.\d+)\}", xml_root_tag)
    if version_search:
        return version_search.group(1)
    else:
        return ""
