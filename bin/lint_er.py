from pathlib import Path
import argparse
import re
import logging
from typing import Literal
from datetime import datetime

LOGGER = logging.getLogger(__name__)

def _configure_logging(args):
    log_fn = datetime.now().strftime('lint_%Y_%m_%d_%H_%M.log')
    log_fpath = Path(args.log_folder + '/' + log_fn)
    if not log_fpath.is_file():
        log_fpath.touch()

    logging.basicConfig(level=logging.WARNING,
                        format = "%(asctime)s - %(levelname)8s - %(message)s",
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=log_fpath,
                        encoding='utf-8'
                        )

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
    parser.add_argument(
        '--log_folder',
        required=True
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
        LOGGER.error(f'{package.name} subfolders should have objects and metadata, found {found}')
        return False

def objects_folder_has_no_access_folder(package: Path) -> bool:
    """An access folder within the objects folder indicates it is an older package,
    and the files within the access folder was created by the Library, and should not be ingested"""
    access_dir = list(package.rglob('access'))

    if access_dir:
        LOGGER.error(f'{package.name} has an access folder in this package: {access_dir}')
        return False
    else:
        return True

def metadata_folder_is_flat(package: Path) -> bool:
    """The metadata folder should not have folder structure"""
    for metadata_path in package.glob('metadata'):
        md_dir_ls = [x for x in metadata_path.iterdir() if x.is_dir()]
    if md_dir_ls:
        LOGGER.error(f'{package.name} has unexpected directory: {md_dir_ls}')
        return False
    else:
        return True

def metadata_folder_has_one_or_less_file(package: Path) -> bool:
    """The metadata folder should have zero to one file"""
    for metadata_path in package.glob('metadata'):
        md_file_ls = [x for x in metadata_path.iterdir() if x.is_file()]
    if len(md_file_ls) > 1:
        LOGGER.warning(f'{package.name} has more than one file in the metadata folder: {md_file_ls}')
        return False
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
                    LOGGER.warning(f"{package.name}: The metadata file, {file.name}, is a TSV file.")
                    return False
                else:
                    LOGGER.warning(f"{package.name} has unknown metadata file, {file.name}")
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

        if good_tsv or unknown_files:
            if good_tsv:
                LOGGER.warning(f"{package.name} metadata folder has FTK TSV files")
            if unknown_files:
                LOGGER.warning(f"{package.name} metadata folder has non-FTK exported files")
            return False

        if any(good_csv):
            LOGGER.warning(f"{package.name}has more than one FTK-exported CSV files")
            return True

    else:
        LOGGER.warning(f"{package.name} has no files in the metadata folder")
        return True

def objects_folder_has_file(package: Path) -> bool:
    """The objects folder must have one or more files, which can be in folder(s)"""
    for objects_path in package.glob('objects'):
        obj_filepaths = [x for x in objects_path.rglob('*') if x.is_file()]

    if not any(obj_filepaths):
        LOGGER.error(f"{package.name} objects folder does not have any file")
        return False
    return True

def package_has_no_bag(package: Path) -> bool:
    """The whole package should not contain any bag"""
    if list(package.rglob('bagit.txt')):
        LOGGER.error(f"{package.name} has bag structure")
        return False
    else:
        return True

def package_has_no_hidden_file(package: Path) -> bool:
    """The package should not have any hidden file"""
    hidden_ls = [h for h in package.rglob('*') if h.name.startswith('.') or
                 h.name.startswith('Thumbs')]
    if hidden_ls:
        LOGGER.warning(f"{package.name} has hidden files {hidden_ls}")
        return False
    else:
        return True

def package_has_no_zero_bytes_file(package: Path) -> bool:
    """The package should not have any zero bytes file"""
    all_file = [f for f in package.rglob('*') if f.is_file()]
    zero_bytes_ls = [f for f in all_file if f.stat().st_size == 0]
    if zero_bytes_ls:
        LOGGER.error(f"{package.name} has zero bytes file {zero_bytes_ls}")
        return False
    else:
        return True

def lint_package(package: Path) -> Literal['valid', 'invalid', 'needs review']:
    """Run all linting tests against a package"""
    result = 'valid'

    less_strict_tests = [
        metadata_folder_has_one_or_less_file,
        metadata_file_has_valid_filename,
        package_has_no_hidden_file
    ]

    for test in less_strict_tests:
        if not test(package):
            result = 'needs review'

    strict_tests = [
        package_has_valid_name,
        package_has_valid_subfolder_names,
        objects_folder_has_no_access_folder,
        metadata_folder_is_flat,
        objects_folder_has_file,
        package_has_no_bag,
        package_has_no_zero_bytes_file
    ]

    for test in strict_tests:
        if not test(package):
            result = 'invalid'

    return result

def main():
    args = parse_args()
    _configure_logging(args)

    valid = []
    invalid = []
    needs_review = []

    counter = 0

    for package in args.packages:
        counter += 1
        result = lint_package(package)
        if result == 'valid':
            valid.append(package.name)
        elif result == 'invalid':
            invalid.append(package.name)
        else:
            needs_review.append(package.name)
    print(f'\nTotal packages ran: {counter}')
    if valid:
        print(f'''
        The following {len(valid)} packages are valid:
        {", ".join(str(x) for x in valid)}''')
    if invalid:
        print(f'''
        The following {len(invalid)} packages are invalid: {invalid}''')
    if needs_review:
        print(f'''
        The following {len(needs_review)} packages need review.
        They may be passed without change after review: {needs_review}''')

if __name__=='__main__':
    main()
