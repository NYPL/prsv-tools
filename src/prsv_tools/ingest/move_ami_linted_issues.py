import argparse
import logging
import shutil
from pathlib import Path

import prsv_tools.ingest.lint_ami as prsvlintami
import prsv_tools.utility.cli as prsvcli

logging.basicConfig(level=logging.INFO)


def parse_args() -> argparse.Namespace:
    parser = prsvcli.Parser()
    parser.add_package()
    parser.add_packagedirectory()
    parser.add_argument(
        "--destination",
        type=prsvcli.extant_dir,
        help="path to a destination directory",
    )
    return parser.parse_args()


def set_dir(package: Path, base_dir: Path, new_folder_name: str):
    if not base_dir or not base_dir.exists():
        logging.error(f"{package.name} not moved - '{base_dir}' does not exist.")
        return

    new_dir = base_dir / new_folder_name / package.name[:3]

    try:
        new_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(package), new_dir)
        logging.info(f"MOVED: '{package.name}' to '{new_dir}' due to issue: {new_folder_name}")
    except PermissionError:
        logging.error(f"'{package.name}' not moved - permission error.")
    except shutil.Error as e:
        logging.error(f"'{package.name}' not moved - {e}")


def get_package_status_and_reason(package: Path) -> tuple[str, str | None]:
    if not prsvlintami.data_folder_has_valid_servicecopies_subfolder(package):
        return "INVALID", "create_scs"
    if not prsvlintami.package_has_valid_name(package):
        return "INVALID", "no_valid_name"
    if not prsvlintami.package_has_valid_subfolder_names(package):
        return "INVALID", "need_valid_subfolder_name"
    if not prsvlintami.data_folder_has_valid_subfolders(package):
        return "INVALID", "need_valid_subfolders"
    if not prsvlintami.data_folder_has_no_empty_folder(package):
        return "INVALID", "empty_folders"
    if not prsvlintami.data_files_are_expected_types(package):
        return "INVALID", "unexpected_file_types"
    if not prsvlintami.tags_folder_is_flat(package):
        return "INVALID", "tags_subfolder"
    if not prsvlintami.tag_file_is_expected_types(package):
        return "INVALID", "tags_unexpected_file_types"
    if not prsvlintami.data_folder_has_no_uncompressed_formats(package):
        return "INVALID", "uncompressed_files"
    if not prsvlintami.data_folder_has_no_part_files(package):
        return "INVALID", "has_parts"
    if not prsvlintami.data_folders_have_at_least_two_files(package):
        return "INVALID", "invalid_file_count"
    if not prsvlintami.package_is_a_bag(package):
        return "INVALID", "not_bagged"
    if not prsvlintami.package_has_no_zero_bytes_file(package):
        return "INVALID", "0byte_files"
    if not prsvlintami.region_files_used_correctly(package):
        return "INVALID", "multiple_regions"


    if not prsvlintami.tags_folder_has_one_to_four_files(package):
        return "NEEDS_REVIEW", "tags_invalid_file_count"
    if not prsvlintami.data_folder_uses_streams(package):
        return "NEEDS_REVIEW", "has_streams"
    if not prsvlintami.package_has_no_hidden_file(package):
        return "NEEDS_REVIEW", "has_hidden_files"

    return "VALID", None


def delete_empty_dir(dir_path: Path):
    if not dir_path.is_dir():
        logging.warning(f"'{dir_path.name}' is not a directory, will not delete.")
        return

    if not any(dir_path.iterdir()):
        try:
            delete_input = input(f"Directory '{dir_path}' is empty. Delete it? (y/n): ").lower()
            if delete_input == "y":
                dir_path.rmdir()
                print(f"Empty directory '{dir_path}' has been deleted.")
        except OSError as e:
            print(f"Error deleting '{dir_path}': {e}")
    else:
        logging.info(f"Directory '{dir_path}' is not empty and will not be deleted.")


def main():
    args = parse_args()
    
    parent_dirs = set(pkg.parent for pkg in args.packages)

    for pkg in args.packages:
        status, reason = get_package_status_and_reason(pkg)

        if status == "VALID":
            logging.info(f"VALID: '{pkg.name}' is valid and has not been moved.")
        elif status in ("INVALID", "NEEDS_REVIEW"):
            set_dir(pkg, args.destination, reason)
        else:
            logging.error(f"'{pkg.name}' has not been moved due to no pkg status.")
    
    print("\n--- Checking source directories ---")
    for parent_dir in parent_dirs:
        delete_empty_dir(parent_dir)


if __name__ == "__main__":
    main()