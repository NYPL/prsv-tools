import os
import pytest
from pathlib import Path

import prsv_tools.ingest.validate_ami_ingests as validate_ami

@pytest.fixture
def valid_local_package(tmp_path, mocker):
    ami_dir = Path(tmp_path / "123456")
    ami_dir.mkdir(parents=True, exist_ok=True)

    pm_folder = ami_dir.joinpath("data/PreservationMasters")
    pm_folder.mkdir(parents=True, exist_ok=True)
    sc_folder = ami_dir.joinpath("data/ServiceCopies")
    sc_folder.mkdir(parents=True, exist_ok=True)

    pm_filepath = pm_folder.joinpath("mym_123456_v01_pm.wav")
    pmjson_filepath = pm_folder.joinpath("mym_123456_v01_pm.json")

    sc_filepath = sc_folder.joinpath("mym_123456_v01_sc.mp4")
    scjson_filepath = sc_folder.joinpath("mym_123456_v01_sc.json")

    pm_file_bytes = os.urandom(70673415)
    sc_file_bytes = os.urandom(18693172)
    json_file_bytes = os.urandom(2888)

    for file in [pm_filepath, pmjson_filepath]:
        with open(file, "wb") as f:
            f.write(pm_file_bytes)

    for file in [pmjson_filepath, scjson_filepath]:
        with open(file, "wb") as f:
            f.write(json_file_bytes)

    with open(sc_filepath, "wb") as f:
        f.write(sc_file_bytes)

    manifest_file = Path(ami_dir/"manifest-md5.txt")
    manifest_file.write_text("1234567890abcdef1234567890abcdef  data/ServiceCopies/myd_123456_v01_sc.mp4\n1234569870abcdef1234567890abcdef  data/PreservationMasters/mym_123456_v01_pm.wav")

    return ami_dir

@pytest.fixture
def invalid_local_package(tmp_path, mocker):
    ami_dir = Path(tmp_path / "123457")
    ami_dir.mkdir(parents=True, exist_ok=True)

    pm_folder = ami_dir.joinpath("data/PreservationMasters")
    pm_folder.mkdir(parents=True, exist_ok=True)
    sc_folder = ami_dir.joinpath("data/ServiceCopies")
    sc_folder.mkdir(parents=True, exist_ok=True)

    pm_filepath = pm_folder.joinpath("mym_123457_v01_pm.wav")
    pmjson_filepath = pm_folder.joinpath("mym_123457_v01_pm.json")
    scjson_filepath = sc_folder.joinpath("mym_123457_v01_sc.json")

    manifest_file = Path(ami_dir/"manifest-md5.txt")
    manifest_file.write_text("1234569870abcdef1234567890abcdef  data/PreservationMasters/mym_123457_v01_pm.wav")

    pm_file_bytes = os.urandom(70673415)
    json_file_bytes = os.urandom(2888)

    for file in [pm_filepath, pmjson_filepath]:
        with open(file, "wb") as f:
            f.write(pm_file_bytes)

    for file in [pmjson_filepath, scjson_filepath]:
        with open(file, "wb") as f:
            f.write(json_file_bytes)

    return ami_dir

# test_get_auth_token
def test_get_auth_token(mocker):
    mock_get_token = mocker.patch('prsv_tools.ingest.validate_ami_ingests.prsvapi.get_token', return_value="prod-token")
    mock_find_apiversion = mocker.patch('prsv_tools.ingest.validate_ami_ingests.prsvapi.find_apiversion',return_value="8.4")

    token, version = validate_ami.get_auth_token("test_cred")

    mock_get_token.assert_called_once_with("test_cred")
    mock_find_apiversion.assert_called_once_with("test_cred")

    assert token == "prod-token" and version == "8.4"

# test_refresh_auth_token
def test_refresh_auth_token(mocker):
    mock_get_token = mocker.patch('prsv_tools.ingest.validate_ami_ingests.prsvapi.get_token', return_value="new-token")
    mock_find_apiversion = mocker.patch('prsv_tools.ingest.validate_ami_ingests.prsvapi.find_apiversion',return_value="8.4")
    mocker.patch('pathlib.Path.exists', return_value=True)
    mock_unlink = mocker.patch('pathlib.Path.unlink')

    token, version = validate_ami.refresh_auth_token("test_cred")

    mock_unlink.assert_called_once()
    assert token == "new-token" and version == "8.4"

# test_find_matching_dirs
def test_find_matching_dirs():
    root_path = "/Volumes/testlpa/lpa/batch1"
    test_dirs = ["123456", "123", "_reingest", "data", "validate_test", "1234567", "12345a"]

    result = validate_ami._find_matching_dirs(root_path, test_dirs)

    assert result["123456"] == ["/Volumes/testlpa/lpa/batch1/123456"] and len(result.items()) == 1

# test_get_local_files
def test_get_local_files(valid_local_package, mocker):
    ami_id = Path(valid_local_package).name
    test_logger = mocker.Mock()
    files, broken_syms, files_to_check, files_to_ignore = validate_ami.get_local_files(valid_local_package, ami_id, test_logger)

    assert "mym_123456_v01_pm.wav" in files
    assert files["mym_123456_v01_pm.wav"] == 70673415
    assert broken_syms == {}
    assert "mym_123456_v01_pm.json" in files_to_check
    assert "manifest-md5.txt" in files_to_ignore

def test_get_local_checksums(valid_local_package):
    result = validate_ami.get_local_checksums(valid_local_package)

    assert result == {
        "myd_123456_v01_sc.mp4": "1234567890abcdef1234567890abcdef",
        "mym_123456_v01_pm.wav": "1234569870abcdef1234567890abcdef"
    }

# test_get_preservica_objects
def test_get_preservica_objects(mocker):
    # Mocking the complex create_pkg_report calls
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.find_all_children', side_effect=lambda t, v, u, c, i, s, n: i.append({'ref': 'io-1', 'title': 'Test IO'}))
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.get_representation_details', return_value=[{'type': 'Preservation'}])
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.get_generation_details', return_value=['co-1'])
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.get_generation_numbers', return_value=['1'])
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.get_bitstream_details', return_value=[{'filename': 'test.wav', 'filesize': 1234, 'fixity': {'MD5': 'abcdef'}}])
    
    session_mock = mocker.Mock()
    file_data, io_titles = validate_ami.get_preservica_objects("token", "8.4", "uuid-1234", session_mock)

    assert "test.wav" in file_data
    assert file_data["test.wav"]["size"] == 1234
    assert file_data["test.wav"]["md5"] == "abcdef"
    assert "Test IO" in io_titles


def test_categorize_files_wav_flac():
    # wav to flac matching
    local_filenames = ["scb_123456_v01f01_pm.wav"]
    prsv_filenames = ["scb_123456_v01f01_pm.flac"]
    preservica_io_titles = set()

    result = validate_ami.categorize_files(local_filenames, prsv_filenames, preservica_io_titles)
    # result = {
    #     "matching": set(),
    #     "missing_local": set(),
    #     "truly_missing": set(),
    #     "found_as_io": set(),
    #     "name_mapping": dict(),
    #     "transcoded": True/False 
    # }

    assert result["transcoded"] == True
    assert result["name_mapping"] == {"scb_123456_v01f01_pm.wav": "scb_123456_v01f01_pm.flac"}
    assert "scb_123456_v01f01_pm.wav" in result["matching"]

def test_categorize_files_mov_mkv():
    # mov to mkv matching
    local_filenames = ["scb_123456_v01f01_pm.mov"]
    prsv_filenames = ["scb_123456_v01f01_pm.mkv"]
    preservica_io_titles = set()

    result = validate_ami.categorize_files(local_filenames, prsv_filenames, preservica_io_titles)

    assert result["transcoded"] == True
    assert result["name_mapping"] == {"scb_123456_v01f01_pm.mov": "scb_123456_v01f01_pm.mkv"}
    assert "scb_123456_v01f01_pm.mov" in result["matching"]

def test_categorize_files_prsv_missing():
    # missing from prsv
    local_filenames = ["myd_789101_v01f02_sc.mp4"]
    prsv_filenames= []
    preservica_io_titles = set()

    result = validate_ami.categorize_files(local_filenames, prsv_filenames, preservica_io_titles)

    assert "myd_789101_v01f02_sc.mp4" in result["truly_missing"]

def test_categorize_files_local_missing():
    # missing from prsv
    local_filenames = []
    prsv_filenames= ["myd_789101_v01f02_sc.mp4"]
    preservica_io_titles = set()

    result = validate_ami.categorize_files(local_filenames, prsv_filenames, preservica_io_titles)

    assert "myd_789101_v01f02_sc.mp4" in result["missing_local"]

# test_check_sizes
def test_check_sizes(mocker):
    cats = {
        "matching": {"test.wav"},
        "name_mapping": {"test.wav": "test.wav"}
    }
    local_files = {"test.wav": 500}
    preservica_files = {"test.wav": {"size": 400, "md5": "bad-md5"}}
    local_checksums = {"test.wav": "good-md5"}
    test_logger = mocker.Mock()

    zero_byte, size_miss, check_miss = validate_ami.check_sizes(cats, local_files, preservica_files, local_checksums, test_logger)

    assert size_miss == [("test.wav", 500, 400)]
    assert check_miss == [("test.wav", "good-md5", "bad-md5")]
    assert zero_byte == []

def test_check_sizes_json_smaller(mocker):
    test_logger = mocker.Mock()
    cats = {"matching": {"meta.json"}, "name_mapping": {"meta.json": "meta.json"}}
    local_files = {"meta.json": 100}
    preservica_files = {"meta.json": {"size": 500, "md5": "abc"}}
    
    zero_byte, size_miss, check_miss = validate_ami.check_sizes(cats, local_files, preservica_files, {"meta.json": "abc"}, test_logger)
    
    assert size_miss == []
    test_logger.warning.assert_called_with("JSON file size mismatch, local smaller than Preservica. Marking valid. Filename: meta.json")

def test_log_verbose_manifest_messages(mocker):
    manifest_logger = mocker.Mock()
    cats = {
        "matching": {"test.wav"}, "name_mapping": {"test.wav": "test.wav"},
        "found_as_io": {"io_only.mkv"}, "truly_missing": {"missing.mp4"}, "missing_local": set()
    }
    local_files = {"test.wav": 1000}
    
    validate_ami.log_verbose_manifest("123456", cats, local_files, {"test.wav": {"size": 1000}}, {}, manifest_logger, ["test.wav"], [])
    
    calls = [call[0][0] for call in manifest_logger.info.call_args_list]
    print(calls)

    assert any("io_only.mkv" in log and "[FOUND AS IO TITLE ONLY]" in log for log in calls)
    assert any("missing.mp4" in log and "[TRULY MISSING IN PRESERVICA]" in log for log in calls)

def test_move_packages(mocker, tmp_path):
    mock_shutil = mocker.patch('prsv_tools.ingest.validate_ami_ingests.shutil.move')
    source_path = tmp_path / "Audio" / "batch_name" / "123456"
    dest_path = tmp_path / "destination"
    
    validate_ami.move_pkgs(source_path, dest_path)
    
    mock_shutil.assert_called_once()

# test_validate_package
def test_validate_package_full_match(mocker, valid_local_package, caplog):
    local_checksums = {"scb_123456_v01f01_pm.mkv":"1234567890abcdef1234567890abcdef"}
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_auth_token', return_value=("test-token", "8.4"))
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_checksums', return_value=local_checksums)
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_preservica_objects', return_value=({"scb_123456_v01f01_pm.mkv": {"size": 100, "md5": "1234567890abcdef1234567890abcdef"}}, set()))
    
    mock_get_local_files = mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_files', return_value=({"scb_123456_v01f01_pm.mkv": 100}, {}, ["scb_123456_v01f01_pm.mkv"], []))
    
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.requests_retry_session')
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.get_single_ami_uuid', return_value="test-uuid")
    
    test_logger = mocker.Mock()
    ami_id = "123456"

    valid_result, broken_syms, reason, deletion_folder = validate_ami.validate_package(ami_id, [valid_local_package], "test-creds", "test-parent-uuid", test_logger, test_logger, test_logger)
    
    log_msg = f"SUCCESS: {ami_id} fully validated (1 files)."
    test_logger.info.assert_any_call(log_msg)

    assert valid_result == True
    assert broken_syms == {}
    assert reason == ""
    assert deletion_folder == False
    assert mock_get_local_files.called

def test_validate_package_multiple_folders(mocker):
    test_logger = mocker.Mock()
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_files', return_value=({"file.wav": 100}, {}, ["file.wav"], []))
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_checksums', return_value={})
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_auth_token', side_effect=Exception("Stop test here"))
    
    try:
        validate_ami.validate_package("123456", ["path/one", "path/two"], "test-creds", "uuid", test_logger, test_logger, test_logger)
    except Exception:
        pass

    test_logger.warning.assert_any_call("Multiple local folders found for 123456, checking content of: path/one")

def test_validate_package_no_local_files(mocker):
    test_logger = mocker.Mock()
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_files', return_value=({}, {}, [], []))
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_checksums', return_value={})
    
    valid_result, broken_syms, reason, deletion_folder = validate_ami.validate_package("123456", ["fake/path"], "creds", "uuid", test_logger, test_logger, test_logger)
    
    assert valid_result == False
    assert reason == "No local files found"
    test_logger.warning.assert_any_call("Skipping 123456: No local files found.")

def test_validate_package_not_in_preservica(mocker):
    test_logger = mocker.Mock()
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_files', return_value=({"file.wav": 100}, {}, ["file.wav"], []))
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_checksums', return_value={})
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_auth_token', return_value=("token", "8.4"))
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.requests_retry_session')
    
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.get_single_ami_uuid', return_value=None)
    
    valid_result, broken_syms, reason, deletion_folder = validate_ami.validate_package("123456", ["fake/path"], "creds", "uuid", test_logger, test_logger, test_logger)
    
    assert valid_result == False
    assert reason == "Not found in Preservica"

def test_validate_package_handles_401(mocker):
    test_logger = mocker.Mock()
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_files', return_value=({"file.wav": 100}, {}, ["file.wav"], []))
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_local_checksums', return_value={})
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_auth_token', return_value=("token", "8.4"))
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.requests_retry_session')
    
    mock_refresh = mocker.patch('prsv_tools.ingest.validate_ami_ingests.refresh_auth_token')
    
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.create_pkg_report.get_single_ami_uuid', side_effect=[Exception("Error 401 Unauthorized"), "real-uuid"])
    mocker.patch('prsv_tools.ingest.validate_ami_ingests.get_preservica_objects', return_value=({"file.wav": {"size": 100}}, set()))
    
    validate_ami.validate_package("123456", ["fake/path"], "creds", "uuid", test_logger, test_logger, test_logger)
    
    test_logger.warning.assert_any_call("401 error for 123456. Refreshing token and retrying package...")
    mock_refresh.assert_called_once_with("creds")