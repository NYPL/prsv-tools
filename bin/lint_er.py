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

def metadata_folder_has_one_or_less_file(package: Path):
    """The metadata folder should have zero to one file"""
    metadata_dir = [x for x in package.iterdir() if x.name == 'metadata'][0]
    md_file_ls = [x for x in metadata_dir.iterdir() if x.is_file()]
    if len(md_file_ls) > 1:
        LOGGER.warning('There are more than one file in the metadata folder')
        return False
    else:
        return True

def metadata_file_has_valid_filename(package: Path):
    """FTK metadata CSV name should conform to M###_(ER|DI|EM)_####.(csv|CSV)"""
    metadata_dir = [x for x in package.iterdir() if x.name == 'metadata'][0]
    md_file_ls = [x for x in metadata_dir.iterdir() if x.is_file()]
    if len(md_file_ls) == 1:
        for file in md_file_ls:
            if re.fullmatch(r'M\d+_(ER|DI|EM)_\d+.(csv|CSV)', file.name):
                return True
            else:
                if re.fullmatch(r'M\d+_(ER|DI|EM)_\d+.(tsv|TSV)', file.name):
                    LOGGER.warning(f"The metadata file, {file.name}, is a TSV file.")
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
        
def objects_folder_has_file(package: Path):
    objects_dir = [x for x in package.iterdir() if x.name == 'objects'][0]
    obj_file = [x for x in objects_dir.iterdir() if x.is_file()]
    if not any(obj_file):
        LOGGER.error("The objects folder does not have any file")
        return False
    return True

def package_has_no_bag(package: Path):
    if list(package.rglob('manifest-md5.txt')):
        LOGGER.error("The package has bag structure")
        return False
    else:
        return True

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
