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
    parser.add_argument(
        "--ignore_regions",
        "-r",
        action='store_true',
        help="ignore packages that have multiple regions",
    )
    return parser.parse_args()


def set_dir(package: Path, base_dir: Path, new_folder_name: str):
    if not base_dir or not base_dir.exists():
        logging.error(f"{package.name} not moved - '{base_dir}' does not exist.")
        return
    if package.parent.name in ["Audio", "Film", "Video", "CD", "DVD", "Data", "VCD"]:
        new_dir = base_dir / new_folder_name / package.parent.parent.name 
        # # debug input
        # new_dir_input = input(f"Move '{package.name}' to '{new_dir}' for issue: {new_folder_name}? (y/n): ").lower()
        # if new_dir_input != "y":
            # logging.info(f"'{package.name}' not moved.")
            # sys.exit()
    else:
        new_dir = base_dir / new_folder_name / package.parent.name

    try:
        new_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(package), new_dir)
        logging.info(f"MOVED: '{package.name}' to '{new_dir}' due to issue: {new_folder_name}")
    except PermissionError:
        logging.error(f"'{package.name}' not moved - permission error.")
    except shutil.Error as e:
        logging.error(f"'{package.name}' not moved - {e}")


def get_package_status_and_reason(package: Path, ignore_regions: bool = False) -> tuple[str, str | None]:
    if not prsvlintami.data_folder_has_valid_servicecopies_subfolder(package):
        return "INVALID", "create_scs"
    if not prsvlintami.servicecopies_folder_has_media_files(package):
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

    if not prsvlintami.region_files_used_correctly(package) and not ignore_regions:
        return "INVALID", "multiple_regions"
    elif not prsvlintami.region_files_used_correctly(package) and ignore_regions:
        return "NEEDS_REVIEW", None
    

    if not prsvlintami.tags_folder_has_one_to_four_files(package):
        return "INVALID", "tags_invalid_file_count"
    if not prsvlintami.data_folder_uses_streams(package):
        return "NEEDS_REVIEW", "has_streams"
    if not prsvlintami.package_has_no_hidden_file(package):
        return "INVALID", "has_hidden_files"
    if not prsvlintami.data_folder_has_valid_preservation_masters_folder(package):
        return "INVALID", "missing_pms"
    if not prsvlintami.preservationmasters_folder_has_media_files(package):
        return "INVALID", "missing_pms"

    return "VALID", None


def delete_empty_dir(dir_path: Path):
    if not dir_path.is_dir():
        logging.warning(f"'{dir_path.name}' is not a directory, will not delete.")
        return

    contents = list(dir_path.iterdir())
    
    is_empty = len(contents) == 0
    is_ds_store_only = len(contents) == 1 and contents[0].name == ".DS_Store"

    if is_empty or is_ds_store_only:
        try:
            prompt_msg = f"Directory '{dir_path}' contains only .DS_Store." if is_ds_store_only else f"Directory '{dir_path}' is empty."
            logging.info(f"{prompt_msg} - Deleting directory.")
            if is_ds_store_only:
                contents[0].unlink()

            dir_path.rmdir()
            logging.info(f"Empty directory '{dir_path}' has been deleted.")

            if dir_path.name in ["Audio", "Film", "Video"]:
                try:
                    dir_path.parent.rmdir()
                    logging.info(f"Empty parent directory '{dir_path.parent}' has been deleted.")
                except OSError:
                    pass
        except OSError as e:
            print(f"Error deleting '{dir_path}': {e}")
    else:
        logging.info(f"Directory '{dir_path}' is not empty and will not be deleted.")


def main():
    args = parse_args()
    
    parent_dirs = set(pkg.parent for pkg in args.packages)

    ignore_regions = True if args.ignore_regions else False

    for pkg in sorted(args.packages):
        status, reason = get_package_status_and_reason(pkg, ignore_regions)

        if status == "VALID":
            logging.info(f"VALID: '{pkg.name}' is valid & has not been moved.")
        elif status == "NEEDS_REVIEW":
            logging.warning(f"NEEDS REVIEW: {pkg.name} has streams or multiple regions, but has not been moved.")
        elif status == "INVALID" and reason is not None:
            set_dir(pkg, args.destination, reason)
        else:
            logging.error(f"'{pkg.name}' has not been moved due to no pkg status.")
    
    print("\nDeleting empty directories:\n")
    for parent_dir in parent_dirs:
        delete_empty_dir(parent_dir)


if __name__ == "__main__":
    main()