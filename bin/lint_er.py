from pathlib import Path
import argparse
import re
import logging
from typing import Literal

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

def package_has_valid_name(package: Path) -> bool:
    """Top level folder name has to conform to M###_(ER|DI|EM)_####"""
    folder_name = package.name
    match = re.fullmatch(r'M\d+_(ER|DI|EM)_\d+', folder_name)

    if match:
        return True
    else:
        LOGGER.error(f'{folder_name} does not conform to M###_(ER|DI|EM)_####')
        return False

def package_has_valid_subfolder_names(package: Path) -> bool:
    """Second level folders must have objects and metadata folder"""
    expected = set(['objects', 'metadata'])
    found = set([x.name for x in package.iterdir()])

    if expected == found:
        return True
    else:
        LOGGER.error(f'Subfolders should have objects and metadata, found {found}')
        return False

def objects_folder_has_no_access_folder(package: Path) -> bool:
    """An access folder within the objects folder indicates it is an older package,
    and the files within the access folder was created by the Library, and should not be ingested"""
    access_dir = list(package.rglob('access'))

    if access_dir:
        LOGGER.error(f'There is an access folder in this package: {access_dir}')
        return False
    else:
        return True

def metadata_folder_is_flat(package: Path) -> bool:
    """The metadata folder should not have folder structure"""
    for metadata_path in package.glob('metadata'):
        md_dir_ls = [x for x in metadata_path.iterdir() if x.is_dir()]
    if not md_dir_ls:
        return True
    elif len(md_dir_ls) == 1 and md_dir_ls[0].name == 'submissionDocumentation':
        LOGGER.error(f'The metadata folder has submissionDocumentation folder')
        return False
    elif len(md_dir_ls) != 0:
        LOGGER.error(f'The metadata folder has unexpected directory: {md_dir_ls}')
        return False

def metadata_folder_has_one_or_less_file(package: Path) -> bool:
    """The metadata folder should have zero to one file"""
    for metadata_path in package.glob('metadata'):
        md_file_ls = [x for x in metadata_path.iterdir() if x.is_file()]
    if len(md_file_ls) > 1:
        LOGGER.warning(f'There are more than one file in the metadata folder: {md_file_ls}')
        return 'review'
    else:
        return True

def metadata_file_has_valid_filename(package: Path) -> bool:
    """FTK metadata CSV name should conform to M###_(ER|DI|EM)_####.(csv|CSV)"""
    for metadata_path in package.glob('metadata'):
        md_file_ls = [x for x in metadata_path.iterdir() if x.is_file()]
    if len(md_file_ls) == 1:
        for file in md_file_ls:
            if re.fullmatch(r'M\d+_(ER|DI|EM)_\d+.(csv|CSV)', file.name):
                return True
            else:
                if re.fullmatch(r'M\d+_(ER|DI|EM)_\d+.(tsv|TSV)', file.name):
                    LOGGER.warning(f"The metadata file, {file.name}, is a TSV file.")
                    return False
                else:
                    LOGGER.warning(f"Unknown metadata file, {file.name}")
                    return False
    elif len(md_file_ls) > 1:
        good_csv = []
        good_tsv = []
        unknown_files = []
        for file in md_file_ls:
            if re.fullmatch(r'M\d+_(ER|DI|EM)_\d+.(csv|CSV)', file.name):
                good_csv.append(file)
            elif re.fullmatch(r'M\d+_(ER|DI|EM)_\d+.(tsv|TSV)', file.name):
                good_tsv.append(file)
            else:
                unknown_files.append(file)
        if any(good_tsv):
            LOGGER.warning("The metadata folder has FTK TSV files")
        if any(unknown_files):
            LOGGER.warning("The metadata folder has non-FTK exported files")
        if any(good_csv):
            LOGGER.warning("There are more than one FTK-exported CSV files")
        if any(good_tsv) or any(unknown_files) or any(good_csv):
            return False
    else:
        LOGGER.warning("There are no files in the metadata folder")
        return True

def objects_folder_has_file(package: Path) -> bool:
    """The objects folder must have one or more files, which can be in folder(s)"""
    for objects_path in package.glob('objects'):
        obj_filepaths = [x for x in objects_path.rglob('*') if x.is_file()]

    if not any(obj_filepaths):
        LOGGER.error("The objects folder does not have any file")
        return False
    return True

def package_has_no_bag(package: Path) -> bool:
    """The whole package should not contain any bag"""
    if list(package.rglob('bagit.txt')):
        LOGGER.error("The package has bag structure")
        return False
    else:
        return True

def package_has_no_hidden_file(package: Path) -> bool:
    """The package should not have any hidden file"""
    hidden_ls = [h for h in package.rglob('*') if h.name.startswith('.') or
                 h.name.startswith('Thumbs')]
    if hidden_ls:
        LOGGER.warning(f"The package has hidden files {hidden_ls}")
        return False
    else:
        return True

def package_has_no_zero_bytes_file(package: Path) -> bool:
    """The package should not have any zero bytes file"""
    all_file = [f for f in package.rglob('*') if f.is_file()]
    zero_bytes_ls = [f for f in all_file if f.stat().st_size == 0]
    if zero_bytes_ls:
        LOGGER.error(f"The package has zero bytes file {zero_bytes_ls}")
        return False
    else:
        return True

def lint_package(package: Path) -> Literal['valid', 'invalid', 'needs review']:
    """Run all linting tests against a package"""
    ls_result = []
    ls_result.append(package_has_valid_name(package))
    ls_result.append(package_has_valid_subfolder_names(package))
    ls_result.append(objects_folder_has_no_access_folder(package))
    ls_result.append(metadata_folder_is_flat(package))
    ls_result.append(metadata_folder_has_one_or_less_file(package))
    ls_result.append(metadata_file_has_valid_filename(package))
    ls_result.append(objects_folder_has_file(package))
    ls_result.append(package_has_no_bag(package))
    ls_result.append(package_has_no_hidden_file(package))
    ls_result.append(package_has_no_zero_bytes_file(package))

    if not False in ls_result or 'review' in ls_result:
        return True
    elif False in ls_result:
        return False
    elif 'review' in ls_result:
        return 'Package includes warning message(s). Review before ingest.'

def main():
    args = parse_args()

    if not lint_package():
        print('package did not lint')

    return False

if __name__=='__main__':
    main()
