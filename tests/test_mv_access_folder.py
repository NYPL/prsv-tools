#!/usr/bin/env python3

import shutil
from pathlib import Path

import pytest

import prsv_tools.ingest.mv_access_folder as mv_access_folder

# Unit tests
# Argument tests
def test_package_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "sys.argv",
        ["script", "--package", str(tmp_path)],
    )

    args = mv_access_folder.parse_args()

    assert tmp_path in args.packages


def test_directory_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    child_dir = tmp_path / "child"
    child_dir.mkdir()

    monkeypatch.setattr(
        "sys.argv",
        ["script", "--directory", str(tmp_path)],
    )

    args = mv_access_folder.parse_args()

    assert child_dir in args.packages

# moving access folder tests
@pytest.fixture
def package_with_access(tmp_path: Path):
    pkg = tmp_path.joinpath("M12345_ER_0001")
    access_folder = pkg / "objects" / "access"
    access_folder.mkdir(parents=True)

    f_metadata = pkg.joinpath("metadata")
    f_metadata.mkdir()

    access_file = access_folder / "textaccessfile.txt"
    access_file.touch()
    access_file.write_bytes(b"some bytes for the access file")

    return pkg


def test_get_access_path(package_with_access):
    """Test that get_access_path function returns a Path object
    when 'access' folder exists in the 'objects' folder"""
    access_path = mv_access_folder.get_access_path(package_with_access)

    result = isinstance(access_path, Path)
    # The isinstance() function returns whether the object is of the specified type.

    assert result

def test_no_access_path(package_with_access):
    """Test that get_access_path returns None when
    the access folder does not exist"""
    no_access_package = package_with_access

    access = no_access_package / "objects" / "access"
    shutil.rmtree(access)

    access_path = mv_access_folder.get_access_path(no_access_package)

    assert not access_path

def test_mv_access_path(package_with_access):
    """Test that mv_access_path does move the access folder
    one level up, including the file inside the folder"""
    access_path = package_with_access / "objects" / "access"

    mv_access_folder.mv_access_path(package_with_access, access_path)

    new_access_path = package_with_access / "access"
    new_access_file_path = package_with_access / "access" / "textaccessfile.txt"

    assert not access_path.exists()
    assert new_access_path.exists()
    assert new_access_file_path.exists()