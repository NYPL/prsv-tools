from pathlib import Path

import pytest

import prsv_tools.ingest.lint_er as lint_er


# Unit tests
# Argument tests
def test_package_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "sys.argv",
        ["script", "--package", str(tmp_path)],
    )

    args = lint_er.parse_args()

    assert tmp_path in args.packages


def test_directory_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    child_dir = tmp_path / "child"
    child_dir.mkdir()

    monkeypatch.setattr(
        "sys.argv",
        ["script", "--directory", str(tmp_path)],
    )

    args = lint_er.parse_args()

    assert child_dir in args.packages


def test_log_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "sys.argv",
        ["../bin/flatten_er_metadata_folder.py", "--log_folder", str(tmp_path)],
    )

    args = lint_er.parse_args()

    assert tmp_path == args.log_folder


# linting tests
@pytest.fixture
def good_package(tmp_path: Path):
    pkg = tmp_path.joinpath("M12345_ER_0001")
    f_object = pkg.joinpath("objects")
    f_object.mkdir(parents=True)
    object_filepath = f_object.joinpath("randomFile.txt")
    object_filepath.touch()
    object_filepath.write_bytes(b"some bytes for object")

    f_metadata = pkg.joinpath("metadata")
    f_metadata.mkdir()

    metadata_filepath = f_metadata.joinpath("M12345_ER_0001.csv")
    metadata_filepath.touch()
    metadata_filepath.write_bytes(b"some bytes for metadata")

    return pkg

@pytest.fixture
def good_package_access(tmp_path: Path):
    pkg = tmp_path.joinpath("M12345_ER_0002")
    f_object = pkg.joinpath("objects")
    f_object.mkdir(parents=True)
    object_filepath = f_object.joinpath("randomFile.wpd")
    object_filepath.touch()
    object_filepath.write_bytes(b"some bytes for object")

    f_access = f_object / "access"
    f_access.mkdir()
    access_file = f_access / "randomFile.wpd.txt"
    access_file.touch()
    access_file.write_bytes(b"some bytes for access file")

    f_metadata = pkg.joinpath("metadata")
    f_metadata.mkdir()

    metadata_filepath = f_metadata.joinpath("M12345_ER_0002.csv")
    metadata_filepath.touch()
    metadata_filepath.write_bytes(b"some bytes for metadata")

    return pkg

def test_top_folder_valid_name(good_package):
    """Top level folder name has to conform to M###_(ER|DI|EM)_####"""
    result = lint_er.package_has_valid_name(good_package)

    assert result


def test_top_folder_invalid_name(good_package):
    """Test that package fails function when the top level folder name
    does not conform to the naming convention, M###_(ER|DI|EM)_####"""
    bad_package = good_package
    bad_package = bad_package.rename(bad_package.parent / "M12345")

    result = lint_er.package_has_valid_name(bad_package)

    assert not result


def test_sec_level_folder_valid_names(good_package, good_package_access):
    """Second level folders must only have objects and metadata folder"""
    result_a = lint_er.package_has_valid_subfolder_names(good_package)
    result_b = lint_er.package_has_valid_subfolder_names(good_package_access)

    assert result_a
    assert result_b


def test_sec_level_folder_invalid_names(good_package):
    """Test that package fails function when second level folders are not named
    objects and metadata, OR when there are more folders other than
    the objects and metadata folders."""
    bad_package = good_package
    objects_path = bad_package / "objects"
    objects_path.rename(bad_package / "obj")

    result = lint_er.package_has_valid_subfolder_names(bad_package)

    assert not result


def test_objects_folder_has_no_access_folder(good_package):
    """The package should not have an 'access' folder that was created by the Library.
    If these files should be kept, they need to be packaged differently"""
    result = lint_er.objects_folder_has_no_access_folder(good_package)

    assert result


def test_objects_folder_has_access_folder(good_package):
    """Test that package fails function when it includes folder(s) named access"""
    bad_package = good_package
    access_dir = bad_package / "objects" / "access"
    access_dir.mkdir()

    result = lint_er.objects_folder_has_no_access_folder(bad_package)

    assert not result


def test_objects_folder_has_no_empty_folder(good_package):
    """The objects folder in the folder should not have any empty folder"""
    result = lint_er.objects_folder_has_no_empty_folder(good_package)

    assert result


def test_objects_folder_has_empty_folder(good_package):
    """Test that package fails function when its objects folder have empty folder"""
    bad_package = good_package
    empty_dir = bad_package / "objects" / "emptydir"
    empty_dir.mkdir()

    result = lint_er.objects_folder_has_no_empty_folder(bad_package)

    assert not result


def test_metadata_folder_is_flat(good_package):
    """The metadata folder should not have folder structure"""
    result = lint_er.metadata_folder_is_flat(good_package)

    assert result


def test_metadata_folder_has_random_folder(good_package):
    """Test that package fails function when the second-level metadata folder
    has any folder in it"""
    bad_package = good_package
    random_dir = bad_package / "metadata" / "random_dir"
    random_dir.mkdir()

    result = lint_er.metadata_folder_is_flat(bad_package)

    assert not result


def test_metadata_folder_has_submissionDocumentation_folder(good_package):
    """Test that package fails function and gives out correct error message
    when the second-level metadata folder has the submissionDocumentation folder"""
    bad_package = good_package
    random_dir = bad_package / "metadata" / "submissionDocumentation"
    random_dir.mkdir()

    result = lint_er.metadata_folder_is_flat(bad_package)

    assert not result


def test_metadata_folder_has_one_or_less_file(good_package):
    """metadata folder should have zero to one file"""
    result = lint_er.metadata_folder_has_one_or_less_file(good_package)

    assert result


def test_metadata_folder_has_more_than_one_file(good_package):
    """Test that package fails when there are more then one file
    in the second-level metadata folder"""
    bad_package = good_package
    new_md_file = bad_package / "metadata" / "M12345_ER_0002.csv"
    new_md_file.touch()

    result = lint_er.metadata_folder_has_one_or_less_file(bad_package)

    assert not result


def test_metadata_file_is_expected_types(good_package):
    """Test that file(s) in the metadata folder are expected types"""
    result = lint_er.metadata_file_is_expected_types(good_package)

    assert result


def test_metadata_file_is_unexpected_types(good_package):
    """Test that package fails function if the file in the metadata folder
    are not expected types"""
    bad_package = good_package
    metadata_path = bad_package / "metadata"
    for file in [x for x in metadata_path.iterdir() if x.is_file()]:
        file.rename(metadata_path / "random.txt")

    result = lint_er.metadata_file_is_expected_types(bad_package)

    assert not result


def test_FTK_metadata_file_valid_name(good_package):
    """FTK metadata CSV/TSV name should conform to M###_(ER|DI|EM)_####.[ct]sv"""
    result = lint_er.metadata_FTK_file_has_valid_filename(good_package)

    assert result


def test_FTK_metadata_file_invalid_name(good_package):
    """Test that package fails function when the FTK metadata file name
    does not conform to the naming convention, M###_(ER|DI|EM)_####.[ct]sv"""
    bad_package = good_package
    metadata_path = bad_package / "metadata"
    for file in [x for x in metadata_path.iterdir() if x.is_file()]:
        file.rename(metadata_path / "M12345-0001.csv")

    result = lint_er.metadata_FTK_file_has_valid_filename(bad_package)

    assert not result


def test_objects_folder_has_file(good_package):
    """The objects folder must have one or more files, which can be in folder(s)"""
    result = lint_er.objects_folder_has_file(good_package)

    assert result


def test_objects_folder_has_no_file(good_package):
    """Test that package fails function when there is no file at all
    within the second-level objects folder"""
    bad_package = good_package
    obj_filepaths = [x for x in bad_package.glob("objects/*") if x.is_file()]
    for file in obj_filepaths:
        file.unlink()
    result = lint_er.objects_folder_has_file(bad_package)

    assert not result


def test_package_has_no_bag(good_package):
    """The package should not have bag structures"""
    result = lint_er.package_has_no_bag(good_package)

    assert result


def test_package_has_bag(good_package):
    """Test that package fails function when there is any bagit.txt file,
    indicating bag structure exists in the package"""
    bad_package = good_package
    bag_folder = bad_package / "objects" / "bagfolder"
    bag_folder.mkdir()
    bag_file = bag_folder.joinpath("bagit.txt")
    bag_file.touch()

    result = lint_er.package_has_no_bag(bad_package)

    assert not result


def test_package_has_no_hidden_file(good_package):
    """The package should not have any hidden file"""
    result = lint_er.package_has_no_hidden_file(good_package)

    assert result


def test_package_has_hidden_file(good_package):
    """Test that package fails function when there is any hidden file"""
    bad_package = good_package
    folder = bad_package / "objects" / "folder"
    folder.mkdir()
    hidden_file = folder.joinpath(".DS_Store")
    hidden_file.touch()

    result = lint_er.package_has_no_hidden_file(bad_package)

    assert not result


def test_package_has_no_zero_bytes_file(good_package):
    """The package should not have any zero bytes file"""
    result = lint_er.package_has_no_zero_bytes_file(good_package)

    assert result


def test_package_has_zero_bytes_file(good_package):
    """Test that package fails function when there is any zero bytes file"""
    bad_package = good_package
    zero_bytes = bad_package / "objects" / "zerobytes.txt"
    zero_bytes.touch()

    result = lint_er.package_has_no_zero_bytes_file(bad_package)

    assert not result


def test_valid_package(good_package):
    """Test that package returns 'valid' when all tests are passed"""
    result = lint_er.lint_package(good_package)

    assert result == "valid"


def test_invalid_package(good_package):
    """Test that package returns 'invalid' when all tests are passed"""
    bad_package = good_package

    bag_folder = bad_package / "objects" / "bagfolder"
    bag_folder.mkdir()
    bag_file = bag_folder.joinpath("bagit.txt")
    bag_file.touch()

    result = lint_er.lint_package(bad_package)

    assert result == "invalid"


def test_unclear_package(good_package):
    """Test that package returns 'needs review' when all tests are passed"""
    bad_package = good_package
    bad_package.joinpath("metadata/M12345_ER_0002.csv").write_text("a")

    result = lint_er.lint_package(bad_package)

    assert result == "needs review"


# Functional tests
def test_lint_valid_package(monkeypatch, good_package, capsys, tmp_path):
    """Run entire script with valid ER"""

    monkeypatch.setattr(
        "sys.argv",
        [
            "../bin/lint_er.py",
            "--package",
            str(good_package),
            "--log_folder",
            str(tmp_path),
        ],
    )

    lint_er.main()

    stdout = capsys.readouterr().out

    assert f"packages are valid: {[str(good_package.name)]}" in stdout


def test_lint_invalid_package(monkeypatch, good_package, capsys, tmp_path):
    """Run entire script with invalid ER"""

    bad_package = good_package

    bag_folder = bad_package / "objects" / "bagfolder"
    bag_folder.mkdir()
    bag_file = bag_folder.joinpath("bagit.txt")
    bag_file.touch()

    monkeypatch.setattr(
        "sys.argv",
        [
            "../bin/lint_er.py",
            "--package",
            str(bad_package),
            "--log_folder",
            str(tmp_path),
        ],
    )

    lint_er.main()

    stdout = capsys.readouterr().out

    assert f"packages are invalid: {[str(bad_package.name)]}" in stdout
