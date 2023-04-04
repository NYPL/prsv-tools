#!/usr/bin/env python3

import argparse
import bagit
import logging
from pathlib import Path
import re


def main():
    '''
    1. Go through every package (e.g. M1234_ER_0001)
    2. Get the path for submissionDocumentation
    3. Check if the subissionDocumentation folder has any files
        (a) if yes, move it / them up one level (naming convention will be checked with the linter)
        (b) if not, delete the folder (this function should be reusable)
    '''


if __name__ == "__main__":
    main()