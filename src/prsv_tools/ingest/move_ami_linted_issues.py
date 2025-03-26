import argparse
import logging
import shutil
from pathlib import Path

import prsv_tools.ingest.lint_ami as prsvlintami
import prsv_tools.utility.cli as prsvcli

logging.basicConfig(level=logging.INFO)


def parse_args() -> argparse.Namespace:
    """Validate and return command-line args"""

    parser = prsvcli.Parser()

    parser.add_package()
    parser.add_packagedirectory()

    parser.add_argument(
        "--destination",
        type=prsvcli.extant_dir,
        help="path to a destination directory",
    )

    return parser.parse_args()


def set_dir(base_dir: Path, package: Path, new_folder_name: str):
    if not base_dir.exists():
        logging.error(f"{package.name} not moved - target path does not exists.")
        return
    new_dir = base_dir / new_folder_name / package.name[:3]

    try:
        new_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(package, new_dir)
        print(package, new_dir)
        logging.info(f"{package.name} has been moved to {new_dir}.")
    except PermissionError:
        logging.error(f"{package.name} not moved - permission error.")
    except shutil.Error:
        logging.error(
            f"{package.name} not moved - file already exists in destination path."
        )


def move_ifs(package: Path, destination: Path):
    base_dir = destination
    new_folder_name = ""
    # pkg != valid sc subfolder
    if prsvlintami.data_folder_has_valid_servicecopies_subfolder(package) == False:
        new_folder_name = "create_scs"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # pkg != valid name
    elif prsvlintami.package_has_valid_name(package) == False:
        new_folder_name = "no_valid_name"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # pkg != valid subfolder name
    elif prsvlintami.package_has_valid_subfolder_names(package) == False:
        new_folder_name = "need_valid_subfolder_name"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # pkg != valid subfolders
    elif prsvlintami.data_folder_has_valid_subfolders(package) == False:
        new_folder_name = "need_valid_subfolders"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # has empty folders
    elif prsvlintami.data_folder_has_no_empty_folder(package) == False:
        new_folder_name = "empty_folders"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # unexpected file types
    elif prsvlintami.data_files_are_expected_types(package) == False:
        new_folder_name = "unexpected_file_types"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # tags does not have subfolders
    elif prsvlintami.tags_folder_is_flat(package) == False:
        new_folder_name = "tags_subfolder"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # tags folder != 0-3 files \\less strict
    elif prsvlintami.tags_folder_has_one_to_four_files(package) == False:
        new_folder_name = "tags_invalid_file_count"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # tags != expected file types
    elif prsvlintami.tag_file_is_expected_types(package) == False:
        new_folder_name = "tags_unexpected_file_types"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # uncompressed wavs/movs
    elif prsvlintami.data_folder_has_no_uncompressed_formats(package) == False:
        new_folder_name = "uncompressed_files"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # has parts
    elif prsvlintami.data_folder_has_no_part_files(package) == False:
        new_folder_name = "has_parts"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # pkg folder(s) have invalid file count
    elif prsvlintami.data_folders_have_at_least_two_files(package) == False:
        new_folder_name = "invalid_file_count"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # not bagged
    elif prsvlintami.package_is_a_bag(package) == False:
        new_folder_name = "not_bagged"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # has 0byte files
    elif prsvlintami.package_has_no_zero_bytes_file(package) == False:
        new_folder_name = "0byte_files"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # has multiple regions
    elif prsvlintami.region_files_used_correctly(package) == False:
        new_folder_name = "multiple_regions"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # has streams \\less strict
    elif prsvlintami.data_folder_uses_streams(package) == False:
        new_folder_name = "has_streams"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    # has hidden files \\less strict
    elif prsvlintami.package_has_no_hidden_file(package) == False:
        new_folder_name = "has_hidden_files"
        result = set_dir(base_dir, package, new_folder_name)
        return result, new_folder_name

    else:
        logging.error(f"{package} has not been moved.")


def main():
    args = parse_args()

    invalid, needs_review, valid = prsvlintami.lint_packages(args.packages)

    for pkg in valid:
        logging.info(f"{pkg} : VALID, has not been moved.")

    for pkg in invalid:
        result = move_ifs(pkg, args.destination)
        logging.info(f"{pkg} : INVALID, has been moved to {result[1]}.")

    if pkg not in invalid and pkg in needs_review:
        result = move_ifs(pkg, args.destination)
        logging.info(f"{pkg} : NEEDS REVIEW, has been moved to {result[1]}.")


if __name__ == "__main__":
    main()
