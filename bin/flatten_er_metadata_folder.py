#!/usr/bin/env python3

import argparse
import bagit
import logging
from pathlib import Path
import re

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

def package_has_submissionDocumentation_folder(package: Path) -> bool:
    """Check if the package has the submisionDocumentation folder under metadata"""
    expected = package / 'metadata' / 'submissionDocumentation'
    if expected.is_dir():
        return True
    else:
        return False



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
        if package_has_submissionDocumentation_folder(package):



if __name__ == "__main__":
    main()