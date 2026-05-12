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
import sqlite3
import time
import json

import prsv_tools.manage.create_pkg_report as create_pkg_report
import prsv_tools.utility.api as prsvapi
import prsv_tools.utility.cli as prsvcli
from prsv_tools.utility.get_md5 import calculate_md5
from prsv_tools.utility.fuzzy_match import fuzzy_compare
import prsv_tools.ingest.move_ami_linted_issues as mv_issues
import prsv_tools.manage.prsv_move as prsv_move

TOKEN_LOCK = threading.Lock()
LOG_LOCK = threading.Lock()

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
    parser = prsvcli.Parser()
    parser.add_package()
    parser.add_packagedirectory()
    parser.add_argument(
        "--destination",
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


def get_local_files(ami_path, ami_id, logger):
    """Returns dict of {filename: filesize} and {filename: full_path}."""
    files = {}
    local_paths = {}
    broken_syms = {}

    files_to_check = []
    files_to_ignore = []
    
    for root, dirs, filenames in os.walk(ami_path, followlinks=True):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for name in filenames:
            if name.startswith(".") or name.endswith(".old") or name.lower() in IGNORED_FILES:
                files_to_ignore.append(name)
                continue
            
            files_to_check.append(name)
            
            full_path = Path(root) / name
            
            if full_path.is_symlink():
                resolved_target = full_path.resolve()
                if not resolved_target.exists():
                    corrected_path = str(resolved_target).replace("/source/", "/Volumes/") 
                    resolved_target = Path(corrected_path)
                
                if resolved_target.exists() and resolved_target.is_file():
                    files[name] = resolved_target.stat().st_size
                    local_paths[name] = str(resolved_target)
                else:
                    logging.error(f"Target missing or unmounted for: {name} -> {resolved_target}")
                    broken_syms[name] = str(resolved_target)
                    
            elif full_path.exists() and full_path.is_file():
                files[name] = full_path.stat().st_size
                local_paths[name] = str(full_path.absolute())
            else:
                logging.error(f"File does not exist: {full_path}")
                
    return files, local_paths, broken_syms, files_to_check, files_to_ignore


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
                            file_data[bitstream['filename']] = {
                                'size': bitstream.get('filesize'),
                                'md5': bitstream.get('fixity', {}).get('MD5', '').lower(),
                                'url': bitstream.get('bitstream_url'),
                                'co_ref': co_ref
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
        
        if f.lower().endswith('.mov'):
            p_name = f[:-4] + '.mkv'
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


def perform_fuzzy_json_match(local_json_path, co_ref, token, session, logger):
    """Fetches JSON from Preservica and performs a fuzzy comparison with local JSON.
    Includes diagnostic logging for network errors."""
    content_url = f"https://nypl.preservica.com/api/entity/content-objects/{co_ref}/generations/latest-active/bitstreams/1/content"
    headers = {
        "Preservica-Access-Token": token,
        "Accept-Encoding": "identity",
        "charset": "UTF-8",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    max_retries = 3
    retry_delay = 3

    for attempt in range(max_retries):
        response = None
        content = b""
        try:
            # Diagnostic: Log request details
            masked_headers = headers.copy()
            if "Preservica-Access-Token" in masked_headers:
                masked_headers["Preservica-Access-Token"] = "REDACTED"
            logger.debug(f"Fuzzy Match Request URL: {content_url}")
            logger.debug(f"Fuzzy Match Request Headers: {masked_headers}")

            # Use the shared session to maintain cookies (JSESSIONID, AWSALB)
            response = session.get(content_url, headers=headers, timeout=60, stream=True)
            
            # Diagnostic: Log response status and headers
            logger.debug(f"Fuzzy Match Response Status: {response.status_code}")
            logger.debug(f"Fuzzy Match Response Headers: {dict(response.headers)}")
            
            # Read byte-by-byte to capture exactly what's happening
            for chunk in response.iter_content(chunk_size=1):
                if chunk:
                    content += chunk
            
            # Now check status after reading content to capture error bodies
            response.raise_for_status()
            
            prsv_data = json.loads(content.decode("utf-8"))

            with open(local_json_path, "r", encoding="utf-8") as f:
                local_data = json.load(f)

            return fuzzy_compare(local_data, prsv_data)
        except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError, UnicodeDecodeError, Exception) as e:
            partial_content = ""
            if content:
                try:
                    partial_content = content.decode("utf-8", errors="replace")[:1000]
                except:
                    partial_content = f"Binary data (Length: {len(content)})"
            
            header_info = dict(response.headers) if response is not None else "No headers"
            status_code = response.status_code if response is not None else "Unknown"
            
            if attempt < max_retries - 1:
                logger.warning(
                    f"Attempt {attempt + 1} failed for {local_json_path}. Status: {status_code}. "
                    f"Error: {e}. Bytes read: {len(content)}. Partial content: {partial_content}. Retrying..."
                )
                time.sleep(retry_delay)
            else:
                logger.error(
                    f"All attempts failed for {local_json_path}. Status: {status_code}. "
                    f"Error: {e}. Bytes read: {len(content)}. Headers: {header_info}. "
                    f"Partial content: {partial_content}"
                )
                return [f"Error during fuzzy match: {e}"]
        finally:
            if response is not None:
                response.close()


def check_sizes(cats, local_files, preservica_files, local_checksums, logger, token, session, local_paths, recalculated_matches=None):
    if recalculated_matches is None:
        recalculated_matches = set()
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
            size_mismatch_found = local_size != prsv_size
            checksum_mismatch_found = local_md5 and prsv_md5 and local_md5 != prsv_md5

            if size_mismatch_found or checksum_mismatch_found:
                # If it's a JSON file, try fuzzy matching before flagging it
                if fname.lower().endswith('.json'):
                    co_ref = prsv_file_info.get('co_ref')
                    if co_ref and fname in local_paths:
                        diffs = perform_fuzzy_json_match(local_paths[fname], co_ref, token, session, logger)
                        if not diffs:
                            logger.info(f"JSON fuzzy match successful for {fname}. Marking as valid.")
                            continue
                        else:
                            print("JSON failed fuzzy matching, see log file.")
                            logger.error(f"JSON fuzzy match failed for {fname}. Differences:")
                            for d in diffs:
                                logger.error(f" - {d}")

                if size_mismatch_found:
                    if local_size < prsv_size and fname.lower().endswith('.json'):
                        logger.warning(f"JSON file size mismatch, local smaller than Preservica. Marking valid. Filename: {fname}")
                    else:
                        size_mismatch.append((fname, local_size, prsv_size))

                if checksum_mismatch_found:
                    if fname in recalculated_matches:
                        continue
                    checksum_mismatch.append((fname, local_md5, prsv_md5))

    return zero_byte_files, size_mismatch, checksum_mismatch


def log_verbose_manifest(ami_id, cats, local_files, preservica_files, local_checksums, manifest_logger, files_checked, files_ignored, recalculated_matches=None):
    if recalculated_matches is None:
        recalculated_matches = set()
    with LOG_LOCK:
        manifest_logger.info(f"Checking package: {ami_id}")
        manifest_logger.info(f"Checking: {', '.join(files_checked)}")
        manifest_logger.info(f"Ignoring: {', '.join(files_ignored) if files_ignored else '--'}")
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
                    if f in recalculated_matches:
                        status_msg = f"{f} [MD5 in local manifest-md5.txt was incorrect but calculated local MD5 matches Preservica]"
                    else:
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

def setup_database(db_path: Path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS validations (
            ami_id TEXT PRIMARY KEY,
            batch TEXT,
            source_path TEXT,
            validation_date TEXT,
            result TEXT,
            fail_reason TEXT,
            log_name TEXT
        )
    ''')
    conn.commit()
    return conn

def update_database(conn, ami_id, source_path, validation_date, result, reason, log_name):
    cursor = conn.cursor()
    batch_name = str(f"{Path(source_path).parent.parent.name}_{Path(source_path).parent.name}") if any(x in Path(source_path).parent.name for x in ["Film", "Audio", "Data", "Video"]) else str(Path(source_path).parent.name)
    cursor.execute('''
        INSERT OR REPLACE INTO validations (ami_id, batch, source_path, validation_date, result, fail_reason, log_name)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (ami_id, batch_name, source_path, validation_date, result, reason, log_name))
    conn.commit()

def validate_package(ami_id, ami_paths, credentials_name, parent_uuid, logger, list_logger, manifest_logger):
    DELETION_SEARCH_UUID = "836a114b-839a-4af8-a4e1-001f200d6d40"
    in_deletion_folder = False
    
    broken_syms = {}
    ami_path = Path(ami_paths[0])
    
    if len(ami_paths) > 1:
        logger.warning(f"Multiple local folders found for {ami_id}, checking content of: {ami_path}")

    local_files, local_paths, broken_syms, files_checked, files_ignored = get_local_files(ami_path, ami_id, logger)
    local_checksums = get_local_checksums(ami_path)
    
    if not local_files:
        logger.warning(f"Skipping {ami_id}: No local files found.")
        return False, broken_syms, "No local files found", in_deletion_folder

    preservica_files, preservica_io_titles = {}, set()
    api_success = False
    max_retries = 1

    for attempt in range(max_retries + 1):
        try:
            token, version = get_auth_token(credentials_name)
            session = create_pkg_report.requests_retry_session()
            
            package_uuid = create_pkg_report.get_single_ami_uuid(token, ami_id, parent_uuid, session)
            
            if not package_uuid:
                package_uuid = create_pkg_report.get_single_ami_uuid(token, ami_id, DELETION_SEARCH_UUID, session, silent=True)
                if package_uuid:
                    in_deletion_folder = True
                    deletion_folder_name = create_pkg_report.get_parent_so_title(token, package_uuid, version, session)
                    logger.warning(f"{ami_id} found in Deletion Folder: '{deletion_folder_name}'")
            
            if not package_uuid:
                logger.error(f"{ami_id} not found in Preservica")
                return False, broken_syms, "Not found in Preservica", in_deletion_folder

            preservica_files, preservica_io_titles = get_preservica_objects(
                token, version, package_uuid, session
            )
            api_success = True
            break
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 and attempt < max_retries:
                logger.warning(f"401 error for {ami_id}. Refreshing token and retrying package...")
                refresh_auth_token(credentials_name)
                continue
            logger.error(f"HTTP Error retrieving {ami_id}: {e}")
            return False, broken_syms, "HTTP Error retrieving files", in_deletion_folder
        except Exception as e:
            if '401' in str(e) and attempt < max_retries:
                logger.warning(f"401 error for {ami_id}. Refreshing token and retrying package...")
                refresh_auth_token(credentials_name)
                continue
            logger.error(f"Error retrieving files for {ami_id}: {e}")
            return False, broken_syms, "Error retrieving files from Preservica", in_deletion_folder

    if not api_success:
        return False, broken_syms, "Failed to connect to Preservica", in_deletion_folder

    recalculated_matches = set()
    for fname, l_md5 in local_checksums.items():
        if fname in preservica_files:
            prsv_md5 = preservica_files[fname].get("md5")
            if l_md5 and prsv_md5 and l_md5 != prsv_md5:
                if fname in local_paths:
                    logger.warning(
                        f"MD5 mismatch for {fname} (Manifest: {l_md5}, Prsv: {prsv_md5}). Recalculating local MD5..."
                    )
                    new_md5 = calculate_md5(Path(local_paths[fname]))
                    if new_md5 == prsv_md5:
                        logger.warning(
                            f"Recalculated MD5 for {fname} matches Preservica. Marking as valid."
                        )
                        recalculated_matches.add(fname)
                    else:
                        logger.error(
                            f"Recalculated MD5 for {fname} ({new_md5}) still does not match Preservica ({prsv_md5})"
                        )

    cats = categorize_files(
        local_files.keys(), preservica_files.keys(), preservica_io_titles
    )
    log_verbose_manifest(
        ami_id,
        cats,
        local_files,
        preservica_files,
        local_checksums,
        manifest_logger,
        files_checked,
        files_ignored,
        recalculated_matches,
    )

    if cats["truly_missing"]:
        logger.error(
            f"FAILED: {ami_id} missing {len(cats['truly_missing'])} file(s) in Preservica."
        )
        for f in sorted(cats["truly_missing"]):
            list_logger.info(f" - {f}")
        if cats["found_as_io"]:
            for f in sorted(cats["found_as_io"]):
                list_logger.info(f" ~ {f} (IO Title Match)")
        return False, broken_syms, "Files missing in Preservica", in_deletion_folder

    elif cats["found_as_io"]:
        logger.warning(
            f"WARNING: {ami_id} - {len(cats['found_as_io'])} files matched IO Titles but NOT Bitstream filenames."
        )
        for f in sorted(cats["found_as_io"]):
            list_logger.info(f" ~ {f} (IO Title Match Only)")
        return False, broken_syms, "Bitstream mismatch (IO Title match only)", in_deletion_folder

    zero_byte_files, size_mismatch, checksum_mismatch = check_sizes(
        cats, local_files, preservica_files, local_checksums, logger, token, session, local_paths, recalculated_matches
    )

    if zero_byte_files:
        logger.error(f"FAILED: {ami_id} contains {len(zero_byte_files)} 0-byte file(s).")
        for f in sorted(zero_byte_files):
            list_logger.info(f" ! {f} (0 Bytes)")
        return False, broken_syms, "0-byte files in Preservica", in_deletion_folder

    if size_mismatch:
        logger.error(f"FAILED: {ami_id} contains {len(size_mismatch)} size mismatch(es).")
        for fname, l_size, p_size in sorted(size_mismatch):
            list_logger.info(f" ! {fname} | Local: {l_size} bytes | Prsv: {p_size} bytes")
        return False, broken_syms, "File size mismatch", in_deletion_folder
    
    if checksum_mismatch:
        logger.error(f"FAILED: {ami_id} contains {len(checksum_mismatch)} checksum mismatch(es).")
        for fname, l_md5, p_md5 in sorted(checksum_mismatch):
            list_logger.info(f" ! {fname} | Local MD5: {l_md5} | Prsv MD5: {p_md5}")
        return False, broken_syms, "File checksum mismatch", in_deletion_folder

    if cats["missing_local"]:
        logger.warning(f"WARNING: {ami_id} has {len(cats['missing_local'])} extra file(s) in Preservica.")
        for f in sorted(cats["missing_local"]):
            if f.endswith("_sc.mp4") or "_sc." in f:
                list_logger.info(f" ~ {f} (Service copy file in Preservica)")
            elif f.startswith("._"):
                list_logger.error(f" + Hidden file in Preservica, not in local.")
                return False, broken_syms, "Hidden file in Preservica", in_deletion_folder
            else:
                list_logger.info(f" + {f} (Extra in Preservica)")

    if cats["transcoded"]:
        logger.info(f"SUCCESS: {ami_id} fully validated ({len(local_files)} files). [.wav to .flac matching]")
    else:
        logger.info(f"SUCCESS: {ami_id} fully validated ({len(local_files)} files).")
        
    return True, broken_syms, "", in_deletion_folder


def main():
    args = parse_args()

    packages_list = sorted(list(args.packages))
    first_source = packages_list[0]

    is_directory_mode = any(arg in sys.argv for arg in ['--directory', '-d'])
    initial_path = first_source.parent if is_directory_mode else first_source
    
    parts = [initial_path.name]
    current = initial_path
    categories = {"Film", "Audio", "Data", "Video"}
    
    while True:
        name = current.name
        is_ami_id = len(name) == 6 and name.isdigit()
        is_category = name in categories and "NYPL" not in name
        
        if (is_ami_id or is_category) and current.parent.name not in ["", "Volumes", "/"]:
            current = current.parent
            parts.insert(0, current.name)
        else:
            break
            
    batch_name = "_".join(parts)
    log_name = f"ingest_validation_{batch_name}.log"
    log_path = Path(args.log_path / log_name) if args.log_path else Path(f"logs/{log_name}")
    logger, list_logger, manifest_logger = setup_logging(log_path, args.verbose)
    
    parent_uuid = AMI_UUID_TEST if "test" in args.credentials else AMI_UUID_PROD 
    if len(packages_list) < 25:
        logger.info(f"Scanning input directories: {', '.join(str(p) for p in packages_list)}")
    else:
        logger.info(f"Scanning {len(packages_list)} input directories...")
    ami_packages = {}
    
    for source_path in packages_list:
        if len(source_path.name) == 6 and source_path.name.isdigit():
            ami_packages.setdefault(source_path.name, []).append(str(source_path))
        
        for root, dirs, _ in os.walk(source_path, followlinks=True):
            matches = _find_matching_dirs(root, dirs)
            for ami_id, paths in sorted(matches.items()):
                if ami_id not in ami_packages:
                    ami_packages[ami_id] = paths
                else:
                    for p in paths:
                        if p not in ami_packages[ami_id]:
                            ami_packages[ami_id].append(p)

    if not ami_packages:
        logger.warning("No AMI packages found to process.")
        log_path.unlink()
        sys.exit(0)

    logger.info(f"Found {len(ami_packages)} AMI packages.")

    try:
        get_auth_token(args.credentials)
    except Exception as e:
        logger.error(f"Fatal auth error before starting: {e}")
        log_path.unlink()
        sys.exit(1)

    deleted_pkgs = set()
    valid_ids = set()
    invalid_pkgs = []  # list of tuples: (ami_id, path, reason)

    all_broken_syms = {}
    
    logger.info(f"Starting validation with {args.threads} threads...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = []
        for ami_id, paths in ami_packages.items():
            future = executor.submit(
                validate_package,
                ami_id,
                paths,
                args.credentials,
                parent_uuid,
                logger,
                list_logger,
                manifest_logger
            )
            futures.append(future)
            
        future_to_ami = {f: (ami_id, paths) for f, (ami_id, paths) in zip(futures, ami_packages.items())}

        for future in concurrent.futures.as_completed(future_to_ami):
            ami_id, paths = future_to_ami[future]
            try:
                is_valid, pkg_broken_syms, reason, in_deletion_folder = future.result()

                if in_deletion_folder:
                    deleted_pkgs.add(ami_id)
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

    db_path = log_path.parent.parent/ "validation_db" / "validation_database.db"
    run_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_file_name = log_path.name

    try:
        db_conn = setup_database(db_path)
        logging.info(f"Updating database. @ {db_path}")
        for ami_id in valid_ids:
            src_path = str(ami_packages[ami_id][0])
            update_database(db_conn, ami_id, src_path, run_time, "valid", "", log_file_name)
            
        for ami_id, path, reason in invalid_pkgs:
            update_database(db_conn, ami_id, str(path), run_time, "invalid", reason, log_file_name)
            
        db_conn.close()
    except Exception as e:
        logger.error(f"Failed to update database: {e}")

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
    
    all_parents = set(Path(paths[0]).parent for paths in ami_packages.values())
    for parent in sorted(all_parents):
        mv_issues.delete_empty_dir(parent)

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

    total_pkgs = len(valid_ids) + len(invalid_pkgs)

    list_logger.info(f"\n{'='*60}")
    
    list_logger.info(f"FINAL VALIDATION SUMMARY [Batch / Package: {batch_name}]")

    list_logger.info(f"\nVALID PACKAGES ({len(valid_ids)}/{total_pkgs}): ")

    list_logger.info(f"\nINVALID PACKAGES ({len(invalid_pkgs)}/{total_pkgs}):")
    for ami_id, path, reason in invalid_pkgs:
        del_note = " [FOUND IN DELETION FOLDER]" if ami_id in deleted_pkgs else ""
        list_logger.info(f" - {ami_id} | Path: {path} (FAILED DUE TO: {reason}){del_note}")

    deletions_valid = set()
    deletions_invalid = set()
    invalid_ids_list = [pkg[0] for pkg in invalid_pkgs]
    invalid_ids_set = set(invalid_ids_list) 
    
    for ami_id in deleted_pkgs:
        if ami_id in valid_ids:
            deletions_valid.add(ami_id)
        if ami_id in invalid_ids_set:
            deletions_invalid.add(ami_id)

    digami_valid_ids = [ami_id for ami_id in valid_ids if ami_id not in deletions_valid]
    digami_invalid_ids = [ami_id for ami_id, _, reason in invalid_pkgs if ami_id not in deletions_invalid and reason != "Not found in Preservica"]

    # valid_ids_str = " ".join(sorted(digami_valid_ids))
    invalid_ids_str = " ".join(sorted(digami_invalid_ids))

    # list_logger.info(f"\n\nVALID AMI IDs in DigAMI:\n{valid_ids_str}")
    list_logger.info(f"\nINVALID AMI IDs in DigAMI:\n{invalid_ids_str}")
    
    valid_deletion_ids_str = " ".join(sorted(deletions_valid))
    invalid_deletion_ids_str = " ".join(sorted(deletions_invalid))
    
    list_logger.info(f"\n*VALID* IN DELETION FOLDER ({len(deletions_valid)}/{total_pkgs}): \n{valid_deletion_ids_str}")
    list_logger.info(f"\nINVALID IN DELETION FOLDER ({len(deletions_invalid)}/{total_pkgs}): \n{invalid_deletion_ids_str}")

    list_logger.info(f"\n{'='*60}")

if __name__ == "__main__":
    main()