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




if __name__=='__main__':
    main()