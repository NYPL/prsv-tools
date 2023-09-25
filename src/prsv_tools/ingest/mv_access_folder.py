#!/usr/bin/env python3

import argparse
import logging
from pathlib import Path

import prsv_tools.utility.cli as prsvcli

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    """Validate and return command-line args"""

    parser = prsvcli.Parser()

    parser.add_package()
    parser.add_packagedirectory()

    return parser.parse_args()


def main():
    """
    under the premise that:
    1. each package has been linted by lint_er.py
    2. each package has "access" folder in the "objects" folder
       (this was an old practice used in DA program)
    3. filename matching in "access" and "objects" are to be
       done in lint_er.py

    script logic:
    1. find the path to the access folder
    2. move the access folder one level up
    e.g. a package, M1234_ER_1 would have "access", "objects" and "metadata"
    folder at the end of running this script

    """
    args = parse_args()


if __name__ == "__main__":
    main()