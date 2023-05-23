import time
from pathlib import Path
import configparser
import requests


def get_token(config_input: str) -> str:

    """
    return token string
    check for existing valid token in token file
    if the file does not exist or the token is out of date, create token
    """

    tokenfilepath = config_input + ".token.file"

    my_file = Path(tokenfilepath)
    if my_file.is_file():

        f = open(tokenfilepath) # Open file on read mode
        lines = f.read().split("\n") # Create a list containing all lines
        print(time.time())
        print(float(lines[0]))
        if time.time() - float(lines[0]) > 500:
            sessiontoken = create_token(config_input,tokenfilepath)
            print(sessiontoken)
        else:
            sessiontoken = lines[1]
            print(sessiontoken)
        f.close() # Close file


    if not my_file.is_file():

        sessiontoken = create_token(config_input,tokenfilepath)
        print(sessiontoken)

    return(sessiontoken)



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


