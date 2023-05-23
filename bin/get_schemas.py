import argparse
import requests
import configparser
import xml.etree.ElementTree as ET

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

def parse_schemas_id(response) -> set:
    schemas_ids = set()
    root = ET.fromstring(response.text)
    ns = '{http://preservica.com/AdminAPI/v6.7}'

    for id in root.iter(f'{ns}ApiId'):
        schemas_ids.add(id.text)

    return schemas_ids

def save_schema_xsd(response):
    root = ET.fromstring(response.text)

    try:
        print(root[0].attrib['name'])
    except KeyError:
        print('KeyError')

def main():
    '''
    1. Decide which instance. This points to corresponding .ini
    2. Generate access token for the specified instance
    3. Decide which endpoint to use
    4. Get XML data. May need to get the ID first and then the actual XML file
    5. Write to the machine
    '''
    # config filenames need to be in the same directory and are hard-coded here
    test_config = 'DA_Dev_SMTP.ini'
    prod_config = 'DA_Production_SMTP.ini'
    schemas_url = 'https://nypl.preservica.com/api/admin/schemas'

    args = parse_args()

    if args.instance == 'test':
        config = test_config
        token = generate_access_token(config)
        schemas_res = get_api_results(token, schemas_url)
        schemas_ids = parse_schemas_id(schemas_res)
        for id in schemas_ids:
            schema_content_url = f'{schemas_url}/{id}/content'
            schema_res = get_api_results(token, schema_content_url)
            # save_schema_xsd(schema_res) -> need this function




if __name__=='__main__':
    main()