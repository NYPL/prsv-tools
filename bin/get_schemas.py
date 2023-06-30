import argparse
import requests
import configparser
import xml.etree.ElementTree as ET
from pathlib import Path

# prsvtoken.py needs to be in the same directory for this to work
import prsvtoken

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--instance',
        '-i',
        type=str,
        required=True,
        choices=['test', 'prod'],
        help='Which Preservica instance to access'
    )

    parser.add_argument(
        '--destination_folder_path',
        '-dest',
        type=str,
        required=False,
        help='''Optional. Provide an absolute folder path to save the files
        in the specified folder'''
    )
    return parser.parse_args()

def generate_access_token(config_input: str):
    config = configparser.ConfigParser()
    config.sections()
    config.read(config_input)
    accesstoken = prsvtoken.get_token(config_input)

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
    ids = [ id.text for id in root.iter(f'{ns}ApiId') ]

    name_id_dict = { n:i for (n,i) in zip(names, ids) }

    return name_id_dict

def fetch_and_write_content(token, url, ns, folder, file_extension):
    content_res = get_api_results(token, url)
    content_dict = parse_res_to_dict(content_res, ns)
    for item_name in content_dict:
        item_content_url = f'{url}/{content_dict[item_name]}/content'
        item_res = get_api_results(token, item_content_url)
        filepath = folder.joinpath(folder, f'{item_name}.{file_extension}')
        with open(filepath, 'w') as f:
            f.write(item_res.text)

def main():
    '''
    config filenames need to be in the same directory and are hard-coded here:
        1. namespace (ns) gets updated when Preservica has a version update
        2. schemas_url, documents_url and transforms_url are relatively stable
    '''

    test_config = 'DA_Dev_SMTP.ini'
    prod_config = 'DA_Production_SMTP.ini'
    ns = '{http://preservica.com/AdminAPI/v6.8}'
    schemas_url = 'https://nypl.preservica.com/api/admin/schemas'
    documents_url = 'https://nypl.preservica.com/api/admin/documents'
    transforms_url = 'https://nypl.preservica.com/api/admin/transforms'

    args = parse_args()

    if args.instance == 'test':
        config = test_config
    else:
        config = prod_config

    if args.destination_folder_path:
        folder = Path(args.destination_folder_path)
    else:
        folder = Path(__file__).parent.absolute()

    token = generate_access_token(config)

    # Fetch and write schemas
    fetch_and_write_content(token, schemas_url, ns, folder, 'xsd')

    # Fetch and write documents
    fetch_and_write_content(token, documents_url, ns, folder, 'xml')

    # Fetch and write transforms
    fetch_and_write_content(token, transforms_url, ns, folder, 'xslt')

if __name__=='__main__':
    main()
