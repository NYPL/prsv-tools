import argparse
import requests
import configparser
import xml.etree.ElementTree as ET

# preservicatoken.py needs to be in the same directory for this to work
from preservicatoken import securitytoken




def main():
    '''
    1. Decide which instance. This points to corresponding .ini
    2. Generate access token for the specified instance
    3. Decide which endpoint to use
    4. Get XML data. May need to get the ID first and then the actual XML file
    5. Write to the machine
    '''




if __name__=='__main__':
    main()