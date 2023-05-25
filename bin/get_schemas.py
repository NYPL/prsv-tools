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

def parse_res_to_dict(response, ns) -> dict:
    root = ET.fromstring(response.text)

    names = [ name.text.replace(" ", "_") for name in root.iter(f'{ns}Name') ]
    ids = [ id.text for id in root.iter(f'{ns}ApiId')]

    name_id_dict = { n:i for (n,i) in zip(names, ids)}

    return name_id_dict

def main():
    '''
    1. Decide which instance. This points to corresponding .ini
    2. Generate access token for the specified instance
    3. Decide which endpoint to use
    4. Get XML data. May need to get the ID first and then the actual XML file
    5. Write to the machine
    '''
    # config filenames need to be in the same directory and are hard-coded here
    # namespace (ns) gets updated when Preservica has a version update
    # schemas_url and transforms_url are relatively stable
    test_config = 'DA_Dev_SMTP.ini'
    prod_config = 'DA_Production_SMTP.ini'
    ns = '{http://preservica.com/AdminAPI/v6.8}'
    schemas_url = 'https://nypl.preservica.com/api/admin/schemas'
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

    schemas_res = get_api_results(token, schemas_url)
    schemas_dict = parse_res_to_dict(schemas_res, ns)
    for name in schemas_dict:
        schema_content_url = f'{schemas_url}/{schemas_dict[name]}/content'
        schema_res = get_api_results(token, schema_content_url)
        filepath = folder.joinpath(folder, f'{name}.xsd')
        with open(filepath, 'w') as f:
            f.write(schema_res.text)

    transform_res = get_api_results(token, transforms_url)
    transforms_dict = parse_res_to_dict(transform_res, ns)
    for name in transforms_dict:
        transform_content_url = f'{transforms_url}/{transforms_dict[name]}/content'
        transform_res = get_api_results(token, transform_content_url)
        filepath = folder.joinpath(folder, f'{name}.xslt')
        with open(filepath, 'w') as f:
            f.write(transform_res.text)

if __name__=='__main__':
    main()