import shutil
from pathlib import Path

import pytest

import prsv_tools.ingest.flatten_er_metadata_folder as flatten_md


# Unit tests
# Argument tests
def test_package_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "sys.argv",
        ["script", "--package", str(tmp_path)],
    )

    args = flatten_md.parse_args()

    assert tmp_path in args.packages


def test_directory_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    child_dir = tmp_path / "child"
    child_dir.mkdir()

    monkeypatch.setattr(
        "sys.argv",
        ["script", "--directory", str(tmp_path)],
    )

    args = flatten_md.parse_args()

    assert child_dir in args.packages


# flattening tests
@pytest.fixture
def common_package(tmp_path: Path):
    pkg = tmp_path.joinpath("M12345_ER_0001")
    f_object = pkg.joinpath("objects")
    f_object.mkdir(parents=True)
    f_metadata = pkg.joinpath("metadata")
    f_metadata.mkdir()

    f_submission = f_metadata.joinpath("submissionDocumentation")
    f_submission.mkdir()
    metadata_filepath = f_submission.joinpath("M12345_ER_0001.csv")
    metadata_filepath.touch()
    metadata_filepath.write_bytes(b"some bytes for metadata")

    return pkg


def test_get_submissionDocumentation_path(common_package):
    """Test that get_submissionDocumentation_path function returns a Path object
    when submissionDocumentation folder exists in the metadata folder"""
    subdoc_path = flatten_md.get_submissionDocumentation_path(common_package)
    result = isinstance(subdoc_path, Path)
    # The isinstance() function returns whether the object is of the specified type.

    assert result


def test_nonexistent_subdoc_path(common_package):
    """Test that get_submissionDocumentation_path returns None when
    the submissionDocumentation folder does not exist"""
    uncommon_package = common_package

    for subdoc in uncommon_package.rglob("submissionDocumentation"):
        shutil.rmtree(subdoc)

    subdoc_path = flatten_md.get_submissionDocumentation_path(common_package)
    assert not subdoc_path


def test_get_subdoc_file(common_package):
    """Test that get_subdoc_file returns a list of file(s)"""
    for subdoc in common_package.rglob("submissionDocumentation"):
        file_ls = flatten_md.get_subdoc_file(subdoc)

    if len(file_ls) > 0 and isinstance(file_ls, list):
        result = True
    else:
        result = False

    assert result


def test_empty_subdoc_folder(common_package):
    """Test that get_subdoc_file returns None when folder is empty"""
    uncommon_pkg = common_package
    for subdoc in uncommon_pkg.rglob("submissionDocumentation"):
        for file in subdoc.iterdir():
            file.unlink()

    result = flatten_md.get_subdoc_file(subdoc)

    assert not result


def test_move_subdoc_files_to_mdfolder(common_package):
    """Test that file(s) are moved from submissionDocumentation
    up to directly under metadata folder: (1) there's file under metadata;
    (2) there's no file in submissionDocumentation"""
    for subdoc in common_package.rglob("submissionDocumentation"):
        subdoc_file_ls = [x for x in subdoc.iterdir() if x.is_file()]

    flatten_md.move_subdoc_files_to_mdfolder(subdoc_file_ls)

    for md_path in common_package.rglob("metadata"):
        md_file_ls = [x for x in md_path.iterdir() if x.is_file()]
        for subdoc in md_path.glob("submissionDocumentation"):
            subdoc_ls = [x for x in subdoc.iterdir()]

    if len(md_file_ls) > 0 and len(subdoc_ls) == 0:
        result = True
    else:
        result = False

    assert result


# Functional tests
def test_flatten_package(monkeypatch, common_package, capsys):
    """Run entire script with a common package"""

    monkeypatch.setattr(
        "sys.argv",
        ["../bin/flatten_er_metadata_folder.py", "--package", str(common_package)],
    )

    flatten_md.main()

    stdout = capsys.readouterr().out
    msg = f"Looking into {common_package.name} submissionDocumentation folder"

    assert msg in stdout


def test_flatten_uncommon_package(monkeypatch, common_package, capsys):
    """Run entire script with a package without submissionDocumentation folder"""
    uncommon_package = common_package

    for subdoc in uncommon_package.rglob("submissionDocumentation"):
        shutil.rmtree(subdoc)

    monkeypatch.setattr(
        "sys.argv",
        ["../bin/flatten_er_metadata_folder.py", "--package", str(uncommon_package)],
    )

    flatten_md.main()

    stdout = capsys.readouterr().out
    msg = f"{uncommon_package.name} does not have submissionDocumentation folder"

    assert msg in stdout
