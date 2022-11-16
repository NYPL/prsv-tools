from pathlib import Path
import argparse
import re
import logging

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

def package_has_valid_name(package: Path):
    """Top level folder name has to conform to M###_(ER|DI|EM)_####"""
    folder_name = package.name
    match = re.fullmatch(r'M\d+_(ER|DI|EM)_\d+', folder_name)
    
    if match:
        return True

def package_has_valid_subfolder_names(package: Path):
    """Second level folders must have objects and metadata folder"""
    expected = set(['objects', 'metadata'])
    found = set([x.name for x in package.iterdir()])

    if expected == found:
        return True
    else:
        return False

def metadata_folder_is_flat(package: Path):
    """The metadata folder should not have folder structure"""
    metadata_dir = [x for x in package.iterdir() if x.name == 'metadata'][0]
    md_dir_ls = [x for x in metadata_dir.iterdir() if x.is_dir()]
    
    if not md_dir_ls:
        return True
    elif len(md_dir_ls) == 1 and md_dir_ls[0].name == 'submissionDocumentation':
        LOGGER.error(f'{metadata_dir} has submissionDocumentation folder')
        if any(md_dir_ls[0].iterdir()):
            LOGGER.error(f'{md_dir_ls[0]} has file(s) in it')
    elif len(md_dir_ls) != 0:
        LOGGER.error(f'{metadata_dir} has unexpected directory')

def metadata_file_has_valid_filename(package: Path):
    """FTK metadata CSV name should conform to M###_(ER|DI|EM)_####.(csv|CSV)"""
    metadata_dir = [x for x in package.iterdir() if x.name == 'metadata']
    md_file_ls = [x for x in metadata_dir[0].iterdir() if x.is_file()]
    for file in md_file_ls:
        if re.fullmatch(r'M\d+_(ER|DI|EM)_\d+.(csv|CSV)', file.name):
            return True
        else:
            if re.fullmatch(r'M\d+_(ER|DI|EM)_\d+.(tsv|TSV)', file.name):
                LOGGER.warning(f"The metadata file, {file.name}, is a TSV file.")

def lint_package() -> bool:
    """Run all linting tests against a package"""
    return True

def main():
    args = parse_args()

    if not lint_package():
        print('package did not lint')

    return False

if __name__=='__main__':
    main()
