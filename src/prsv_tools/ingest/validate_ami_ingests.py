import argparse
import os
import sys
import logging
import threading
import concurrent.futures
from datetime import datetime
from pathlib import Path
import shutil
import requests

import prsv_tools.manage.create_pkg_report as create_pkg_report
import prsv_tools.utility.api as prsvapi
import prsv_tools.ingest.move_ami_linted_issues as mv_issues
import prsv_tools.manage.prsv_move as prsv_move

TOKEN_LOCK = threading.Lock()

IGNORED_FILES = {
    ".DS_Store", "premis-events.json", "thumbs.db",
    "manifest-md5.txt", "tagmanifest-md5.txt", "bagit.txt", "bag-info.txt"
}

AMI_UUID_PROD = "183a74b5-7247-4fb2-8184-959366bc0cbc"
AMI_UUID_TEST = ""


def setup_logging(log_file: Path, is_verbose: bool):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    # logfile handler
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(str(log_file), mode='a')
    fh.setFormatter(log_file_formatter)
    logger.addHandler(fh)

    # console handler
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

    # list_logger
    basic_formatter = logging.Formatter('%(message)s')

    list_logger = logging.getLogger('list_logger')
    list_logger.setLevel(logging.INFO)
    list_logger.propagate = False
    if list_logger.hasHandlers():
        list_logger.handlers.clear()
        
    bh = logging.StreamHandler(sys.stdout)
    bh.setFormatter(basic_formatter)
    list_logger.addHandler(bh)

    lh = logging.FileHandler(str(log_file), mode='a')
    lh.setFormatter(basic_formatter)
    list_logger.addHandler(lh)

    manifest_logger = logging.getLogger('manifest_logger')
    manifest_logger.setLevel(logging.INFO)
    manifest_logger.propagate = False
    if manifest_logger.hasHandlers():
        manifest_logger.handlers.clear()

    mh = logging.FileHandler(str(log_file), mode='a')
    mh.setFormatter(basic_formatter)
    manifest_logger.addHandler(mh)

    if is_verbose:
        mch = logging.StreamHandler(sys.stdout)
        mch.setFormatter(basic_formatter)
        manifest_logger.addHandler(mch)
    
    return logger, list_logger, manifest_logger


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", "-s",
        required=True,
        type=Path,
        help="Path to the directory containing AMI packages (6-digit folders)."
    )
    parser.add_argument(
        "--destination", "-d",
        type=Path,
        help="[Optional] Path to sort valid & invalid packages to. (Top level directories containing multiple packages)"
    )
    parser.add_argument(
        "--credentials", "-c",
        required=True,
        help="Credentials set name to use (default: prod-ingest)."
    )
    parser.add_argument(
        "--log_path", "-l",
        required=False,
        type=Path,
        help="Path to save the log file."
    )
    parser.add_argument(
        "--verbose", "-v",
        required=False,
        action='store_true',
        help="Print verbose output."
    )
    parser.add_argument(
        "--threads", "-t",
        required=False,
        type=int,
        default=5,
        help="Number of concurrent threads to use (default: 5)."
    )
    parser.add_argument(
        "--deletion-parent-ref", "-dpf",
        type=str,
        required=False,
        help="[Optional] The parent ref to move packages to if they fail validation."
    )
    return parser.parse_args()

def get_auth_token(credentials_name):
    with TOKEN_LOCK:
        token = prsvapi.get_token(credentials_name)
        version = prsvapi.find_apiversion(credentials_name)
        return token, version

def refresh_auth_token(credentials_name):
    with TOKEN_LOCK:
        token_file = Path(f"{credentials_name}.token.file")
        if token_file.exists():
            try:
                token_file.unlink()
            except OSError:
                pass
        token = prsvapi.get_token(credentials_name)
        version = prsvapi.find_apiversion(credentials_name)
        return token, version

def _find_matching_dirs(root: str, dirs: list) -> dict:
    matches = {}
    for d in dirs:
        if len(d) == 6 and d.isdigit():
            matches.setdefault(d, []).append(os.path.join(root, d))
    return matches

def get_local_files(ami_path):
    """Returns dict of {filename: filesize}."""
    files = {}
    broken_syms = {}
    
    for root, dirs, filenames in os.walk(ami_path, followlinks=True):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for name in filenames:
            if name.startswith(".") or name.endswith(".old") or name.lower() in IGNORED_FILES:
                continue

            full_path = Path(root) / name
            
            if full_path.is_symlink():
                resolved_target = full_path.resolve()
                if not resolved_target.exists():
                    corrected_path = str(resolved_target).replace("/source/", "/Volumes/") 
                    resolved_target = Path(corrected_path)
                
                if resolved_target.exists() and resolved_target.is_file():
                    files[name] = resolved_target.stat().st_size
                else:
                    logging.error(f"Target missing or unmounted for: {name} -> {resolved_target}")
                    broken_syms[name] = str(resolved_target)
                    
            elif full_path.exists() and full_path.is_file():
                files[name] = full_path.stat().st_size
            else:
                logging.error(f"File does not exist: {full_path}")
                
    return files, broken_syms


def get_local_checksums(ami_path):
    """Parses manifest-md5.txt to create a dict of {filename: checksum}."""
    checksums = {}
    manifest_path = Path(ami_path) / "manifest-md5.txt"
    if manifest_path.exists():
        with manifest_path.open('r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 2:
                    filename = Path(parts[-1]).name
                    for part in parts:
                        if len(part) == 32 and part.isalnum():
                            checksums[filename] = part.lower()
                            break
    return checksums


def get_preservica_objects(token, version, package_uuid, session):
    file_data = {}
    io_titles = set()
    namespaces = {
        'xip': f'http://preservica.com/XIP/v{version}',
        'entity': f'http://preservica.com/EntityAPI/v{version}'
    }

    # ignore child SOs list
    child_so_refs = [] 
    io_info_list = []
    
    try:
        create_pkg_report.find_all_children(token, version, package_uuid, child_so_refs, io_info_list, session, namespaces)
    except Exception as e:
        raise Exception(f"Failed finding children: {e}") from e

    for io_info in io_info_list:
        io_ref = io_info['ref']
        if title := io_info.get('title'):
            io_titles.add(title)
        
        reps = create_pkg_report.get_representation_details(token, version, io_ref, session, namespaces)
        for rep in reps:
            co_refs = create_pkg_report.get_generation_details(token, version, io_ref, rep['type'], session, namespaces)
            for co_ref in co_refs:
                generations = create_pkg_report.get_generation_numbers(token, version, co_ref, session, namespaces)
                for generation in generations:
                    bitstreams = create_pkg_report.get_bitstream_details(token, version, co_ref, generation, session, namespaces)
                    for bitstream in bitstreams:
                        if bitstream and bitstream.get('filename'):
                            file_data[bitstream['filename']] = {
                                'size': bitstream.get('filesize'),
                                'md5': bitstream.get('fixity', {}).get('MD5', '').lower()
                            }

    return file_data, io_titles


def categorize_files(local_filenames, prsv_filenames, preservica_io_titles):
    local_set = set(local_filenames)
    prsv_set = set(prsv_filenames)
    
    matching_files = local_set & prsv_set
    local_to_prsv_name = {f: f for f in matching_files}
    use_transcode_match = False

    for f in local_set - matching_files:
        if f.lower().endswith('.wav'):
            p_name = f[:-4] + '.flac'
            if p_name in prsv_set:
                matching_files.add(f)
                local_to_prsv_name[f] = p_name
                use_transcode_match = True
                
    missing_from_local = prsv_set - set(local_to_prsv_name.values())
    missing_from_preservica = local_set - matching_files
    
    found_as_io = missing_from_preservica & preservica_io_titles
    truly_missing = missing_from_preservica - found_as_io

    return {
        "matching": matching_files,
        "missing_local": missing_from_local,
        "truly_missing": truly_missing,
        "found_as_io": found_as_io,
        "name_mapping": local_to_prsv_name,
        "transcoded": use_transcode_match
    }


def check_sizes(cats, local_files, preservica_files, local_checksums, logger):
    size_mismatch = []
    zero_byte_files = []
    checksum_mismatch = []

    for fname in cats["matching"]:
        p_name = cats["name_mapping"][fname]
        local_size = local_files[fname]
        prsv_file_info = preservica_files.get(p_name, {})

        prsv_size = prsv_file_info.get('size', -1)
        prsv_md5 = prsv_file_info.get('md5')
        local_md5 = local_checksums.get(fname)

        if prsv_size == 0:
            zero_byte_files.append(fname)
            continue
            
        is_wav_to_flac = fname.lower().endswith('.wav') and p_name.lower().endswith('.flac')

        if not is_wav_to_flac:
            if local_size != prsv_size:
                if local_size < prsv_size and fname.lower().endswith('.json'):
                    logger.warning(f"JSON file size mismatch, local smaller than Preservica. Marking valid. Filename: {fname}")
                else:
                    size_mismatch.append((fname, local_size, prsv_size))

            if local_md5 and prsv_md5 and local_md5 != prsv_md5:
                checksum_mismatch.append((fname, local_md5, prsv_md5))

    return zero_byte_files, size_mismatch, checksum_mismatch


def log_verbose_manifest(ami_id, cats, local_files, preservica_files, local_checksums, manifest_logger):
    header = f"{'LOCAL':<55} {'PRESERVICA STATUS'}"
    separator = f"{'-'*55} {'-'*40}"
    output = [f"\n{ami_id} VERBOSE MANIFEST:", header, separator]
    print_files = set(local_files.keys()).union(cats["missing_local"])
    
    for f in sorted(print_files):
        if f in cats["matching"]:
            p_name = cats["name_mapping"][f]
            l_size = local_files[f]
            p_info = preservica_files.get(p_name, {})
            p_size = p_info.get('size')
            l_md5 = local_checksums.get(f)
            p_md5 = p_info.get('md5')
            
            if f != p_name:
                status_msg = f"{p_name} [WAV TO FLAC MATCH]" if f.lower().endswith('.wav') else f"{p_name} [FLAC TO WAV MATCH]"
            elif p_size == 0:
                status_msg = f"{f} [0 BYTES IN PRESERVICA]"
            elif l_size != p_size:
                status_msg = f"{f} [SIZE MISMATCH: L={l_size} vs P={p_size}]"
            elif l_md5 and p_md5 and l_md5 != p_md5:
                status_msg = f"{f} [MD5 MISMATCH: L={l_md5[:6]}... vs P={p_md5[:6]}...]"
            else:
                md5_str = "MD5 MATCH" if l_md5 and p_md5 else "NO MD5"
                status_msg = f"{f} ({l_size} bytes | {md5_str})"
            
            output.append(f"{f:<55} --------> {status_msg}")
            
        elif f in cats["found_as_io"]:
            output.append(f"{f:<55} --------> [FOUND AS IO TITLE ONLY]")
        elif f in cats["truly_missing"]:
            output.append(f"{f:<55} --------> [TRULY MISSING IN PRESERVICA]")
        elif f in cats["missing_local"]:
            output.append(f"{'[MISSING LOCALLY]':<55} --------> {f}")
    
    output.append("")
    manifest_logger.info("\n".join(output))


def move_pkgs(source_path: Path, destination_path: Path):
    try:
        parent_name = source_path.parent.name
        if parent_name in {"Audio", "Film", "Video"}:
            target_path = destination_path / source_path.parent.parent.name / source_path.name
        else:
            target_path = destination_path / parent_name / source_path.name

        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_path), str(target_path))
        logging.info(f"Moved '{source_path}' to '{target_path}'")
    except Exception as e:
        logging.error(f"Error moving '{source_path}' to '{destination_path}': {e}")

def validate_package(ami_id, ami_paths, credentials_name, parent_uuid, logger, list_logger, manifest_logger):
    broken_syms = {}
    ami_path = Path(ami_paths[0])
    
    if len(ami_paths) > 1:
        logger.warning(f"Multiple local folders found for {ami_id}, checking content of: {ami_path}")

    logger.info(f"Checking package: {ami_id}")

    local_files, broken_syms = get_local_files(ami_path)
    local_checksums = get_local_checksums(ami_path)
    
    if not local_files:
        logger.warning(f"Skipping {ami_id}: No local files found.")
        return False, broken_syms, "No local files found"

    preservica_files, preservica_io_titles = {}, set()
    api_success = False
    max_retries = 1

    for attempt in range(max_retries + 1):
        try:
            token, version = get_auth_token(credentials_name)
            session = create_pkg_report.requests_retry_session()
            
            package_uuid = create_pkg_report.get_single_ami_uuid(token, ami_id, parent_uuid, session)
            
            if not package_uuid:
                return False, broken_syms, "Not found in Preservica"

            preservica_files, preservica_io_titles = get_preservica_objects(
                token, version, package_uuid, session
            )
            api_success = True
            break #
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 and attempt < max_retries:
                logger.warning(f"401 error for {ami_id}. Refreshing token and retrying package...")
                refresh_auth_token(credentials_name)
                continue
            logger.error(f"HTTP Error retrieving {ami_id}: {e}")
            return False, broken_syms, "HTTP Error retrieving files"
        except Exception as e:
            if '401' in str(e) and attempt < max_retries:
                logger.warning(f"401 error for {ami_id}. Refreshing token and retrying package...")
                refresh_auth_token(credentials_name)
                continue
            logger.error(f"Error retrieving files for {ami_id}: {e}")
            return False, broken_syms, "Error retrieving files from Preservica"

    if not api_success:
        return False, broken_syms, "Failed to connect to Preservica"

    cats = categorize_files(local_files.keys(), preservica_files.keys(), preservica_io_titles)
    log_verbose_manifest(ami_id, cats, local_files, preservica_files, local_checksums, manifest_logger)

    if cats["truly_missing"]:
        logger.error(f"FAILED: {ami_id} missing {len(cats['truly_missing'])} file(s) in Preservica.")
        for f in sorted(cats["truly_missing"]):
            list_logger.info(f" - {f}")
        if cats["found_as_io"]:
            for f in sorted(cats["found_as_io"]):
                list_logger.info(f" ~ {f} (IO Title Match)")
        return False, broken_syms, "Files missing in Preservica"

    elif cats["found_as_io"]:
        logger.warning(f"WARNING: {ami_id} - {len(cats['found_as_io'])} files matched IO Titles but NOT Bitstream filenames.")
        for f in sorted(cats["found_as_io"]):
            list_logger.info(f" ~ {f} (IO Title Match Only)")
        return False, broken_syms, "Bitstream mismatch (IO Title match only)"

    zero_byte_files, size_mismatch, checksum_mismatch = check_sizes(
        cats, local_files, preservica_files, local_checksums, logger
    )

    if zero_byte_files:
        logger.error(f"FAILED: {ami_id} contains {len(zero_byte_files)} 0-byte file(s).")
        for f in sorted(zero_byte_files):
            list_logger.info(f" ! {f} (0 Bytes)")
        return False, broken_syms, "0-byte files in Preservica"

    if size_mismatch:
        logger.error(f"FAILED: {ami_id} contains {len(size_mismatch)} size mismatch(es).")
        for fname, l_size, p_size in sorted(size_mismatch):
            list_logger.info(f" ! {fname} | Local: {l_size} bytes | Prsv: {p_size} bytes")
        return False, broken_syms, "File size mismatch"
    
    if checksum_mismatch:
        logger.error(f"FAILED: {ami_id} contains {len(checksum_mismatch)} checksum mismatch(es).")
        for fname, l_md5, p_md5 in sorted(checksum_mismatch):
            list_logger.info(f" ! {fname} | Local MD5: {l_md5} | Prsv MD5: {p_md5}")
        return False, broken_syms, "File checksum mismatch"

    if cats["missing_local"]:
        logger.warning(f"WARNING: {ami_id} has {len(cats['missing_local'])} extra file(s) in Preservica.")
        for f in sorted(cats["missing_local"]):
            if f.endswith("_sc.mp4") or "_sc" in f:
                list_logger.info(f" ~ {f} (Service copy file in Preservica)")
            else:
                list_logger.info(f" + {f} (Extra in Preservica)")

    if cats["transcoded"]:
        logger.info(f"SUCCESS: {ami_id} fully validated ({len(local_files)} files). [.wav to .flac matching]")
    else:
        logger.info(f"SUCCESS: {ami_id} fully validated ({len(local_files)} files).")
        
    return True, broken_syms, ""


def main():
    args = parse_args()
    log_path = args.log_path if args.log_path else Path(f"logs/ingest_validation_{args.source.name}_{datetime.now().strftime('%Y%m%d')}.log")
    logger, list_logger, manifest_logger = setup_logging(log_path, args.verbose)
    
    parent_uuid = AMI_UUID_TEST if "test" in args.credentials else AMI_UUID_PROD 

    logger.info(f"Scanning source directory: {args.source}")
    ami_packages = {}
    
    for root, dirs, _ in os.walk(args.source, followlinks=True):
        matches = _find_matching_dirs(root, dirs)
        for ami_id, paths in sorted(matches.items()):
            if ami_id not in ami_packages:
                ami_packages[ami_id] = paths
            else:
                ami_packages[ami_id].extend(paths)

    if not ami_packages:
        logger.warning("No AMI packages found to process.")
        sys.exit(0)

    logger.info(f"Found {len(ami_packages)} AMI packages.")

    try:
        get_auth_token(args.credentials)
    except Exception as e:
        logger.error(f"Fatal auth error before starting: {e}")
        sys.exit(1)

    valid_ids = set()
    invalid_pkgs = []  # list of tuples: (ami_id, path, reason)

    all_broken_syms = {}
    
    logger.info(f"Starting validation with {args.threads} threads...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        future_to_ami = {
            executor.submit(
                validate_package, 
                ami_id, 
                paths, 
                args.credentials, 
                parent_uuid,
                logger, 
                list_logger, 
                manifest_logger
            ): (ami_id, paths) 
            for ami_id, paths in ami_packages.items()
        }

        for future in concurrent.futures.as_completed(future_to_ami):
            ami_id, paths = future_to_ami[future]
            try:
                is_valid, pkg_broken_syms, reason = future.result()

                if pkg_broken_syms:
                    all_broken_syms[ami_id] = pkg_broken_syms
                if is_valid:
                    valid_ids.add(ami_id)
                else:
                    invalid_pkgs.append((ami_id, paths[0], reason))
            except Exception as e:
                logger.error(f"Exception generated for {ami_id}: {e}")
                if not any(pkg[0] == ami_id for pkg in invalid_pkgs):
                    invalid_pkgs.append((ami_id, paths[0], f"Exception during validation: {e}"))
    
    logger.info("Validation complete.")

    # --- SUMMARY LOGGING ---
    list_logger.info(f"\n{'='*60}")
    list_logger.info(f"FINAL VALIDATION SUMMARY [Batch / Package: {(args.source).name}]")
    list_logger.info("="*60)

    list_logger.info(f"\nVALID PACKAGES ({len(valid_ids)}/{(len(valid_ids)+len(invalid_pkgs))}): ")
    # for valid_id in sorted(valid_ids):
    #     list_logger.info(f" - {valid_id}")

    list_logger.info(f"\nINVALID PACKAGES ({len(invalid_pkgs)}/{(len(valid_ids)+len(invalid_pkgs))}):")
    for ami_id, path, reason in invalid_pkgs:
        list_logger.info(f" - {ami_id} | Path: {path} (FAILED DUE TO: {reason})")

    # if all_broken_syms:
        # list_logger.info(f"\nPACKAGES WITH BROKEN SYMLINKS ({len(all_broken_syms)}):")
        # for ami_id, syms in all_broken_syms.items():
            # list_logger.info(f" - {ami_id}")
            # for name, target_path in syms.items():
                # list_logger.info(f"     ! {name} (Target missing: {target_path})") 
    if invalid_pkgs and (len(valid_ids) > 0):
        invalid_ids_str = " ".join(ami_id for ami_id, _, _ in invalid_pkgs)
        list_logger.info(f"\nInvalid AMI IDs: {invalid_ids_str}")
    
    list_logger.info(f"\n{'='*60}")

    # --- MOVING LOGIC ---
    if args.destination and (valid_ids or invalid_pkgs):
        # moved_valid = 0
        # moved_invalid = 0
        destination_path = Path(args.destination)
        logger.info(f"\nMoving valid packages to destination: {args.destination}")
        
        for ami_id in sorted(valid_ids):
            for source_path in ami_packages[ami_id]:
                move_pkgs(Path(source_path), destination_path / "_valid")
                
        for ami_id, _, _ in sorted(invalid_pkgs):
            for source_path in ami_packages[ami_id]:
                move_pkgs(Path(source_path), destination_path / "_validation_failed")
    
    mv_issues.delete_empty_dir(args.source)

    if args.deletion_parent_ref and invalid_pkgs:
        logger.info(f"\nMoving {len(invalid_pkgs)} failed packages to deletion folder in Preservica: {args.deletion_parent_ref}")
        invalid_ami_ids = [ami_id for ami_id, _, _ in invalid_pkgs]
        try:
            prsv_move.process_move_list(
                credentials=args.credentials,
                pkg_list=sorted(invalid_ami_ids),
                new_parent_ref=args.deletion_parent_ref,
                existing_logger=logger
            )
        except Exception as e:
            logger.error(f"Failed to execute prsv_move: {e}")

if __name__ == "__main__":
    main()