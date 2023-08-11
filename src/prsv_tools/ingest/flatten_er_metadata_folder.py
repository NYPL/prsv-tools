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


def get_submissionDocumentation_path(package: Path) -> Path | None:
    """Get submissionDocumentation folder path under metadata"""
    expected_path = package / "metadata" / "submissionDocumentation"
    if expected_path.is_dir():
        return expected_path
    else:
        return None


def get_subdoc_file(subdoc: Path) -> list | None:
    """Check whether the submissionDocumentation folder has any files"""
    subdoc_file_ls = [x for x in subdoc.iterdir() if x.is_file()]
    if subdoc_file_ls:
        return subdoc_file_ls
    else:
        return None


def move_subdoc_files_to_mdfolder(subdoc_file_ls: list) -> None:
    """Move file(s) from the submissionDocumentation folder to the metadata folder"""
    for file in subdoc_file_ls:
        dest = file.parent.parent / file.name
        file.rename(dest)
        LOGGER.info(f"Moving {file} to {dest}")


def main():
    """
    1. Go through every package (e.g. M1234_ER_0001)
    2. Get the path for submissionDocumentation
    3. Check if the subissionDocumentation folder has any files
        (a) if yes, move it / them up one level
        (b) if not, delete the folder (this function should be reusable)
    """
    args = parse_args()

    for package in args.packages:
        subdoc_path = get_submissionDocumentation_path(package)
        if subdoc_path:
            subdoc_file_ls = get_subdoc_file(subdoc_path)
            print(f"Looking into {package.name} submissionDocumentation folder")
            if subdoc_file_ls:
                move_subdoc_files_to_mdfolder(subdoc_file_ls)
            try:
                subdoc_path.rmdir()
                # Path.rmdir() only removes empty directory
            except OSError as e:
                LOGGER.error(f"Directory probably not empty {str(e)}")
        else:
            print(f"{package.name} does not have submissionDocumentation folder")


if __name__ == "__main__":
    main()
