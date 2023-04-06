#!/usr/bin/env python3

import argparse
import bagit
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    """Validate and return command-line args"""

    def extant_dir(p):
        path = Path(p)
        if not path.is_dir():
            raise argparse.ArgumentTypeError(
                f'{path} does not exist'
            )
        return path

    def list_of_paths(p):
        path = extant_dir(p)
        child_dirs = []
        for child in path.iterdir():
            if child.is_dir():
                child_dirs.append(child)
        return child_dirs

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--package',
        type=extant_dir,
        nargs='+',
        dest='packages',
        action='extend'
    )
    parser.add_argument(
        '--directory',
        type=list_of_paths,
        dest='packages',
        action='extend'
    )

    return parser.parse_args()

def get_submissionDocumentation_path(package: Path) -> Path | None:
    """Get submissionDocumentation folder path under metadata"""
    expected_path = package / 'metadata' / 'submissionDocumentation'
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
        LOGGER.info(f'Moving {file} to {dest}')

def main():
    '''
    1. Go through every package (e.g. M1234_ER_0001)
    2. Get the path for submissionDocumentation
    3. Check if the subissionDocumentation folder has any files
        (a) if yes, move it / them up one level (naming convention will be checked with the linter)
        (b) if not, delete the folder (this function should be reusable)
    '''
    args = parse_args()

    for package in args.packages:
        subdoc_path = get_submissionDocumentation_path(package)
        if subdoc_path:
            subdoc_file_ls = get_subdoc_file(subdoc_path)
            if subdoc_file_ls:
                move_subdoc_files_to_mdfolder(subdoc_file_ls)
            try:
                subdoc_path.rmdir()
                # Path.rmdir() only removes empty directory
            except OSError as e:
                LOGGER.error(f'Directory probably not empty' + str(e))
        else:
            LOGGER.info(f'{package.name} does not have submissionDocumentation folder')


if __name__ == "__main__":
    main()