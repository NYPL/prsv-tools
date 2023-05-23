import time
from pathlib import Path
import configparser
import requests


def get_token(credential_file_name: str) -> str:

    """
    return token string
    check for existing valid token in token file
    if the file does not exist or the token is out of date, create token
    """

    token_file = Path(f'{credential_file_name}.token.file')

    if token_file.is_file():
        time_issued, sessiontoken = token_file.read_text().split("\n")
        # tokens are valid for 500 seconds
        if time.time() - float(time_issued) < 500:
            return sessiontoken

    create_token(credential_file_name, token_file)
    return get_token(token_file)



#########################################################################################

#get new token function

def create_token(
        config_input: str,
        tokenfilepath: Path
    ) -> None:

    print(config_input)

    print(tokenfilepath)

    #read from config file to get the correct parameters for the token request

    config = configparser.ConfigParser()
    config.sections()

    config.read(config_input)

    url = config['DEFAULT']['URL']
    hostval = config['DEFAULT']['Host']
    usernameval = config['DEFAULT']['Username']
    passwordval = config['DEFAULT']['Password']
    tenantval = config['DEFAULT']['Tenant']

    #build the query string and get a new token

    payload = 'username=' + usernameval + '&password=' + passwordval + '&tenant=' + tenantval

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}


    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.raise_for_status())


    data = response.json()

    tokenval = (data["token"])

    timenow = str(time.time())

    #write token to token.file for later reuse

    tokenfile = open(tokenfilepath, "w")
    tokenfile.write(timenow)
    tokenfile.write("\n")
    tokenfile.write(tokenval)
    tokenfile.close()

    return(tokenval)

#########################################################################################


