import argparse
import requests
import configparser
import xml.etree.ElementTree as ET
from pathlib import Path

# preservicatoken.py needs to be in the same directory for this to work
from preservicatoken import securitytoken

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--instance',
        '-i',
        type=str,
        required=True,
        help='Please type "test" or "prod"'
    )

    parser.add_argument(
        '--destination_folder',
        '-dest',
        type=str,
        required=False,
        help='''Optional. If a folder path is included, the files will be
        saved in the folder'''
    )
    return parser.parse_args()

def generate_access_token(config_input: str):
    config = configparser.ConfigParser()
    config.sections()
    config.read(config_input)
    accesstoken = securitytoken(config_input)

    return accesstoken

def get_api_results(accesstoken, url):
    headers = {
                'Preservica-Access-Token': accesstoken,
                'Content-Type': "application/xml"
              }
    response = requests.request('GET', url, headers=headers)
    return response # response object



def main():
    '''
    1. Decide which instance. This points to corresponding .ini
    2. Generate access token for the specified instance
    3. Decide which endpoint to use
    4. Get XML data.
    5. Parse XML data to Excel spreadsheets
    6. Write to the machine
    '''
    # config filenames need to be in the same directory and are hard-coded here
    # namespace (ns) gets updated when Preservica has a version update
    # schemas_url and transforms_url are relatively stable
    test_config = 'DA_Dev_SMTP.ini'
    prod_config = 'DA_Production_SMTP.ini'
    ns = '{http://preservica.com/AdminAPI/v6.8}'
    permissions_url = 'https://nypl.preservica.com/api/admin/security/permissions'
    transforms_url = 'https://nypl.preservica.com/api/admin/transforms'

    args = parse_args()

    if args.instance == 'test':
        config = test_config
    else:
        config = prod_config

    if args.destination_folder:
        folder = Path(args.destination_folder)
    else:
        folder = Path(__file__).parent.absolute()

    token = generate_access_token(config)

    res = get_api_results(token, permissions_url)
    print(res.text)


if __name__=='__main__':
    main()