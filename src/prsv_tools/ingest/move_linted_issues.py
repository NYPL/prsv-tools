import argparse
import shutil
import logging
from pathlib import Path

import prsv_tools.ingest.lint_ami as prsvlintami
import prsv_tools.utility.cli as prsvcli

logging.basicConfig(level=logging.INFO)

def parse_args() -> argparse.Namespace:
    """Validate and return command-line args"""

    parser = prsvcli.Parser()

    parser.add_package()
    parser.add_packagedirectory()

    return parser.parse_args()


def set_dir(default_dir: Path, base_dir: Path, package: Path, new_folder_name: str):
    new_dir = base_dir / new_folder_name / f"{package[:3]}" 
    if not new_dir.is_dir():
        new_dir = default_dir
    shutil.move(package, new_dir)
    logging.info(f"{package.name} has been moved to {new_dir.name}.")

def move_ifs(package):
    #default_dir used to ensure remaining packages are clear for ingest
    base_dir = Path("/source/lpasync/prsv_prod_ingest/")
    default_dir = base_dir / f"unsorted/{package[:3]}/"
    new_folder_name = ""
    # pkg /= valid sc subfolder
    if prsvlintami.data_folder_has_valid_servicecopies_subfolder(package) == False:
        new_folder_name = "create_scs"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result
    
    # pkg /= valid name
    elif prsvlintami.package_has_valid_name(package) == False:
        new_folder_name = "no_valid_name"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # pkg /= valid subfolder name
    elif prsvlintami.package_has_valid_subfolder_names(package) == False:
        new_folder_name = "need_valid_subfolder_name"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # pkg /= valid subfolders
    elif prsvlintami.data_folder_has_valid_subfolders(package) == False:
        new_folder_name = "need_valid_subfolders"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result


    # has empty folders
    elif prsvlintami.data_folder_has_no_empty_folder(package) == False:
        new_folder_name = "empty_folders"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # unexpected file types
    elif prsvlintami.data_files_are_expected_types(package) == False:
        new_folder_name = "unexpected_file_types"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # tags does not have subfolders
    elif prsvlintami.tags_folder_is_flat(package) == False:
        new_folder_name = "tags_subfolder"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # tags folder /= 0-3 files \\less strict
    elif prsvlintami.tags_folder_has_one_to_four_files(package) == False:
        new_folder_name = "tags_invalid_file_count"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # tags /= expected file types
    elif prsvlintami.tag_file_is_expected_types(package) == False:
        new_folder_name = "tags_unexpected_file_types"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # uncompressed wavs/movs
    elif prsvlintami.data_folder_has_no_uncompressed_formats(package) == False:
        new_folder_name = "uncompressed_files"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # has parts
    elif prsvlintami.data_folder_has_no_part_files(package) == False:
        new_folder_name = "has_parts"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # pkg folder(s) have invalid file count
    elif prsvlintami.data_folders_have_at_least_two_files(package) == False:
        new_folder_name = "invalid_file_count"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # not bagged
    elif prsvlintami.package_is_a_bag(package) == False:
        new_folder_name = "not_bagged"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # has 0byte files
    elif prsvlintami.package_has_no_zero_bytes_file(package) == False:
        new_folder_name = "0byte_files"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # has multiple regions
    elif prsvlintami.region_files_used_correctly(package) == False:
        new_folder_name = "multiple_regions"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # has streams \\less strict
    elif prsvlintami.data_folder_uses_streams(package) == False:
        new_folder_name = "has_streams"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result

    # has hidden files \\less strict
    elif prsvlintami.package_has_no_hidden_file(package) == False:
        new_folder_name = "has_hidden_files"
        result = set_dir(default_dir, base_dir, package, new_folder_name)
        return result
        
    # invalid metadata filename \\not being tested for currently
    # elif prsvlintami.metadata_FTK_file_has_valid_filename(package) == False:
        # new_dir = os.path.join(base_dir, "invalid_metadata_filename")
        # if new_dir.is_dir():
        #     return
        # else: 
        #     new_dir = default_dir
        # shutil.move(package, new_dir)
        # logging.info(f"{package.name} has been moved to {new_dir.name}.")

    else:
        logging.info(f"{package} has not been moved.")

def main():
    args = parse_args()

    packages = args.packages

    invalid = prsvlintami.main[0]
    needs_review = prsvlintami.main[1]

    for package in sorted(packages):
        prsvlintami.lint_package(package)
        for pkg in invalid:
            if package == pkg:
                move_ifs(package)
            else:
                #nesting this for loop as invalid issues > needs_review issues
                for pkg in needs_review:
                    if package == pkg:
                        move_ifs(package)

if __name__ == "__main__":
    main()


