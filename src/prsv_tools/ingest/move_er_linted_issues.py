import argparse
import logging
import shutil
from pathlib import Path

import prsv_tools.ingest.lint_er as lint_er
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
        "--debug",
        action="store_true",
        help="print actions without modifying any files",
    )
    parser.add_argument(
        "--symlink",
        action="store_true",
        help="create a symlink to the package files instead of moving them",
    )
    return parser.parse_args()


def set_dir(package: Path, base_dir: Path, new_folder_name: str, debug: bool = False, symlink: bool = False):
    if not base_dir or not base_dir.exists():
        logging.error(f"{package.name} not moved - '{base_dir}' does not exist.")
        return
    
    new_dir = base_dir / new_folder_name / package.parent.name

    if debug:
        action = "SYMLINK" if symlink else "MOVE"
        logging.info(f"[DEBUG] Would {action} '{package.name}' to '{new_dir}' due to issue: {new_folder_name}")
        return

    try:
        new_dir.mkdir(parents=True, exist_ok=True)
        target_package_dir = new_dir / package.name
        
        if symlink:
            target_package_dir.mkdir(parents=True, exist_ok=True)
            for file_path in package.rglob("*"):
                target_path = target_package_dir / file_path.relative_to(package)
                if file_path.is_dir():
                    target_path.mkdir(parents=True, exist_ok=True)
                else:
                    target_path.symlink_to(file_path.resolve())
            logging.info(f"SYMLINKED: '{package.name}' to '{new_dir}' due to issue: {new_folder_name}")
        else:
            shutil.move(str(package), new_dir)
            logging.info(f"MOVED: '{package.name}' to '{new_dir}' due to issue: {new_folder_name}")
    except PermissionError:
        logging.error(f"'{package.name}' not moved - permission error.")
    except shutil.Error as e:
        logging.error(f"'{package.name}' not moved - {e}")


def get_package_status_and_reason(package: Path) -> tuple[str, str | None]:
    # Strict tests (INVALID)
    if not lint_er.package_has_valid_name(package):
        return "INVALID", "no_valid_name"
    if not lint_er.package_has_valid_subfolder_names(package):
        return "INVALID", "need_valid_subfolder_name"
    if not lint_er.objects_folder_has_no_access_folder(package):
        return "INVALID", "objects_has_access"
    if not lint_er.objects_folder_has_no_empty_folder(package):
        return "INVALID", "empty_folders"
    if not lint_er.metadata_folder_is_flat(package):
        return "INVALID", "metadata_not_flat"
    if not lint_er.metadata_file_is_expected_types(package):
        return "INVALID", "metadata_unexpected_file_types"
    if not lint_er.metadata_FTK_file_has_valid_filename(package):
        return "INVALID", "metadata_ftk_invalid_name"
    if not lint_er.objects_folder_has_file(package):
        return "INVALID", "objects_no_file"
    if not lint_er.package_has_no_bag(package):
        return "INVALID", "has_bag"
    if not lint_er.package_has_no_zero_bytes_file(package):
        return "INVALID", "0byte_files"
    if not lint_er.package_has_no_hidden_file(package):
        return "INVALID", "has_hidden_files"

    # Less strict tests (NEEDS_REVIEW)
    if not lint_er.metadata_folder_has_one_or_less_file(package):
        return "NEEDS_REVIEW", "metadata_file_count"
    if not lint_er.access_files_match_with_objects(package):
        return "NEEDS_REVIEW", "access_match_objects"

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
    
    parent_dirs = set(pkg.parent for pkg in args.packages if "_photograph" not in pkg.name)
    
    needs_review_packages = []

    for pkg in sorted(args.packages):
        if "_photograph" in pkg.name:
            continue
            
        status, reason = get_package_status_and_reason(pkg)

        if status == "VALID":
            logging.info(f"VALID: '{pkg.name}' is valid & has not been moved.")
        elif status == "NEEDS_REVIEW":
            logging.warning(f"NEEDS REVIEW: '{pkg.name}' flagged for '{reason}', but has not been moved.")
            needs_review_packages.append((pkg.name, reason))
        elif status == "INVALID" and reason is not None:
            set_dir(pkg, args.destination, reason, args.debug, args.symlink)
        else:
            logging.error(f"'{pkg.name}' has not been moved due to unknown status.")
    
    if not args.debug and not args.symlink:
        print("\nDeleting empty directories:\n")
        for parent_dir in parent_dirs:
            if parent_dir.exists():
                delete_empty_dir(parent_dir)

    if needs_review_packages:
        print("\nNEEDS REVIEW PACKAGES:\n")
        for name, reason in needs_review_packages:
            print(f"- {name}: {reason}")

if __name__ == "__main__":
    main()
