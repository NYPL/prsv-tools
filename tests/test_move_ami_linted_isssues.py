import logging
from pathlib import Path
from unittest.mock import Mock

import pytest

import prsv_tools.ingest.move_ami_linted_issues as move_ami


def test_package_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "sys.argv",
        ["script", "--package", str(tmp_path)],
    )

    args = move_ami.parse_args()

    assert tmp_path in args.packages


def test_directory_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    child_dir = tmp_path / "child"
    child_dir.mkdir()

    monkeypatch.setattr(
        "sys.argv",
        ["script", "--directory", str(tmp_path)],
    )

    args = move_ami.parse_args()

    assert child_dir in args.packages


@pytest.fixture
def good_package(tmp_path: Path):
    pkg = tmp_path.joinpath("123456")

    pm_folder = pkg.joinpath("data/PreservationMasters")
    pm_folder.mkdir(parents=True)
    sc_folder = pkg.joinpath("data/ServiceCopies")
    sc_folder.mkdir(parents=True)

    pm_filepath = pm_folder.joinpath("mym_123456_v01_pm.flac")
    pmjson_filepath = pm_folder.joinpath("mym_123456_v01_pm.json")

    sc_filepath = sc_folder.joinpath("mym_123456_v01_sc.mp4")
    scjson_filepath = sc_folder.joinpath("mym_123456_v01_sc.json")

    for file in [
        pm_filepath,
        pmjson_filepath,
        sc_filepath,
        scjson_filepath,
        (pkg / "bagit.txt"),
        (pkg / "manifest-md5.txt"),
    ]:
        file.write_bytes(b"some bytes for object")

    f_metadata = pkg.joinpath("tags")
    f_metadata.mkdir()

    metadata_filepath = f_metadata.joinpath("mym_123456_v01_pm.mkv.xml.gz")
    metadata_filepath.touch()
    metadata_filepath.write_bytes(b"some bytes for metadata")

    return pkg


@pytest.fixture
def tmp_destination(tmp_path: Path):
    tmp_dir = tmp_path.joinpath("destination")
    tmp_dir.mkdir()
    return tmp_dir


def test_set_dir_move_as_expected(tmp_destination: Path, good_package: Path, caplog):
    caplog.set_level(logging.INFO)
    new_folder_name = "create_scs"
    new_dir = tmp_destination / new_folder_name
    new_dir.mkdir(parents=True)

    move_ami.set_dir(tmp_destination, good_package, new_folder_name)

    assert (
        tmp_destination / new_folder_name / good_package.name[:3] / good_package.name
    ).exists()

    log_msg = f"{good_package.name} has been moved to {(tmp_destination / new_folder_name / good_package.name[:3])}."
    assert log_msg in caplog.text


def test_set_dir_move_and_create_dir(tmp_destination: Path, good_package: Path):
    new_folder_name = "create_scs"
    new_dir = tmp_destination / new_folder_name

    move_ami.set_dir(tmp_destination, good_package, new_folder_name)

    assert new_dir.exists()
    assert (
        tmp_destination / new_folder_name / good_package.name[:3] / good_package.name
    ).exists()


def test_set_dir_fails_safely_with_bad_permissions(
    tmp_destination: Path, good_package: Path, caplog
):
    new_folder_name = "create_scs"
    tmp_destination.chmod(0o555)

    move_ami.set_dir(tmp_destination, good_package, new_folder_name)

    assert not (
        tmp_destination / new_folder_name / good_package.name[:3] / good_package.name
    ).exists()
    assert good_package.exists()

    log_msg = f"{good_package.name} not moved - permission error."
    assert log_msg in caplog.text


def test_set_dir_fails_safely_with_missing_target(
    tmp_destination: Path, good_package: Path, caplog
):
    new_folder_name = "create_scs"

    move_ami.set_dir(tmp_destination / "does_not_exist", good_package, new_folder_name)

    log_msg = f"{good_package.name} not moved - target path does not exists."

    assert good_package.exists()
    assert log_msg in caplog.text


def test_moved_package_exists(tmp_destination: Path, good_package: Path, caplog):
    new_folder_name = "create_scs"

    new_dir = (
        tmp_destination / new_folder_name / good_package.name[:3] / good_package.name
    )
    new_dir.mkdir(parents=True, exist_ok=True)

    assert new_dir.exists()

    move_ami.set_dir(tmp_destination, good_package, new_folder_name)

    assert good_package.exists()

    log_msg = (
        f"{good_package.name} not moved - file already exists in destination path."
    )

    assert log_msg in caplog.text
