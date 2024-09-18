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

def get_access_path(package: Path) -> Path | None:
    """Get the 'access' folder path under 'objects' folder"""
    expected_path = package / "objects" / "access"

    if expected_path.is_dir():
        return expected_path
    else:
        LOGGER.error(f"access folder does not exist")
        return None

def mv_access_path(package: Path, access_path: Path):
    """Move access folder one level up, alongside with objects and metadata"""
    target = package / "access"

    LOGGER.info(f"Now moving {access_path} to be {target}")
    access_path.rename(target)

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

    for package in args.packages:
        access_path = get_access_path(package)

        mv_access_path(package, access_path)


if __name__ == "__main__":
    main()