from pathlib import Path

import pytest

import prsv_tools.ingest.lint_ami as lint_ami


# Unit tests
# Argument tests
def test_package_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "sys.argv",
        ["script", "--package", str(tmp_path)],
    )

    args = lint_ami.parse_args()

    assert tmp_path in args.packages


def test_directory_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    child_dir = tmp_path / "child"
    child_dir.mkdir()

    monkeypatch.setattr(
        "sys.argv",
        ["script", "--directory", str(tmp_path)],
    )

    args = lint_ami.parse_args()

    assert child_dir in args.packages


def test_log_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "sys.argv",
        ["script", "--log_folder", str(tmp_path)],
    )

    args = lint_ami.parse_args()

    assert tmp_path == args.log_folder


# linting tests
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

    for file in [pm_filepath, pmjson_filepath, sc_filepath, scjson_filepath, (pkg / "bagit.txt"), (pkg / "manifest-md5.txt")]:
        file.write_bytes(b"some bytes for object")

    f_metadata = pkg.joinpath("tags")
    f_metadata.mkdir()

    metadata_filepath = f_metadata.joinpath("mym_123456_v01_pm.mkv.xml.gz")
    metadata_filepath.touch()
    metadata_filepath.write_bytes(b"some bytes for metadata")

    return pkg


def test_top_folder_valid_name(good_package: Path):
    """Top level folder name has to conform to ###### (six digits)"""
    result = lint_ami.package_has_valid_name(good_package)

    assert result


def test_top_folder_invalid_name(good_package: Path):
    """Test that package fails function when the top level folder name
    does not conform to the naming convention, ###### (six digits)"""
    bad_package = good_package
    bad_package = bad_package.rename(bad_package.parent / "12345")

    result = lint_ami.package_has_valid_name(bad_package)

    assert not result


def test_sec_level_folder_valid_names(good_package: Path):
    """Second level folders must only have objects and metadata folder"""
    result = lint_ami.package_has_valid_subfolder_names(good_package)

    assert result


def test_sec_level_folder_invalid_names(good_package: Path):
    """Test that package fails function when second level folders are not named
    data or tags, OR when there are more folders other than
    the objects and metadata folders."""
    bad_package = good_package
    data_path = bad_package / "data"
    data_path.rename(bad_package / "dat")

    result = lint_ami.package_has_valid_subfolder_names(bad_package)

    assert not result


def test_data_folder_has_valid_subfolders(good_package: Path):
    """The package should have third-level folders named PreservationMasters, Mezzanines,
    Editmasters, ServiceCopies, or Images"""
    result = lint_ami.data_folder_has_valid_subfolders(good_package)

    assert result


def test_data_folder_has_invalid_subfolders(good_package: Path):
    """The package should only have third-level folders named PreservationMasters, Mezzanines,
    Editmasters, ServiceCopies, or Images"""
    (good_package / "data" / "Metadata").mkdir()
    result = lint_ami.data_folder_has_valid_subfolders(good_package)

    assert not result


def test_data_folder_has_sc_folder(good_package: Path):
    """Test that package includes folder(s) named servicecopies"""
    result = lint_ami.data_folder_has_valid_servicecopies_subfolder(good_package)

    assert result


def test_data_folder_has_sc_folder(good_package: Path):
    """Test that package fails when it does not include a servicecopies folder"""
    bad_package = good_package

    sc_path = bad_package / "data" / "ServiceCopies"
    sc_path.rename(bad_package / "Mezzanines")

    result = lint_ami.data_folder_has_valid_servicecopies_subfolder(bad_package)

    assert not result


def test_data_folder_has_no_empty_folder(good_package: Path):
    """The data folder in the folder should not have any empty folder"""
    result = lint_ami.data_folder_has_no_empty_folder(good_package)

    assert result


def test_data_folder_has_empty_folder(good_package: Path):
    """Test that package fails function when its data folder has empty folder"""
    bad_package = good_package
    empty_dir = bad_package / "data" / "emptydir"
    empty_dir.mkdir()

    result = lint_ami.data_folder_has_no_empty_folder(bad_package)

    assert not result


def test_data_folder_has_acceptable_extensions(good_package: Path):
    """The package should only have acceptable extensions"""
    result = lint_ami.data_files_are_expected_types(good_package)

    assert result


def test_data_folder_has_unacceptable_extensions(good_package: Path):
    """The package should only have acceptable extensions"""
    bad_package = good_package
    (bad_package / "data" / "bad.xml").touch()

    result = lint_ami.data_files_are_expected_types(bad_package)

    assert not result


def test_tags_folder_is_flat(good_package: Path):
    """The tags folder should not have folder structure"""
    result = lint_ami.tags_folder_is_flat(good_package)

    assert result


def test_tags_folder_has_random_folder(good_package: Path):
    """Test that package fails function when the second-level tags folder
    has any folders in it"""
    bad_package = good_package
    random_dir = bad_package / "tags" / "random_dir"
    random_dir.mkdir()

    result = lint_ami.tags_folder_is_flat(bad_package)

    assert not result


def test_tags_folder_has_too_many_files(good_package: Path):
    """Test that warning is created when tags folder contains too many files"""
    bad_package = good_package
    tags_dir = bad_package / "tags"

    for i in range(4):
        (tags_dir / f"tag{i}.txt").touch()

    result = lint_ami.tags_folder_has_one_to_four_files(bad_package)

    assert not result


def test_metadata_folder_has_one_to_four_files(good_package: Path):
    """tags folder should have 1 to 4 files"""
    result = lint_ami.tags_folder_has_one_to_four_files(good_package)

    assert result


def test_metadata_file_is_expected_types(good_package: Path):
    """Test that file(s) in the metadata folder are expected types"""

    tags_dir = good_package / "tags"

    for i in ["test.framemd5", "test.mkv.xml.gz", "test.ssa"]:
        (tags_dir / i).touch()
    result = lint_ami.tag_file_is_expected_types(good_package)

    assert result


def test_metadata_file_is_unexpected_gz(good_package: Path):
    """Test that file(s) in the metadata folder are expected types"""

    bad_package = good_package
    (bad_package / "tags" / "test.dvrescue.xml.gz").touch()

    result = lint_ami.tag_file_is_expected_types(bad_package)

    assert not result


def test_metadata_file_is_unexpected_types(good_package: Path):
    """Test that package fails function if the file in the tags folder
    are not expected types"""
    bad_package = good_package
    (bad_package / "tags" / "bad.xml").touch()

    result = lint_ami.tag_file_is_expected_types(bad_package)

    assert not result


def test_data_has_no_uncompressed(good_package: Path):
    """Return True when data folder has uncompressed formats (wav, mov)"""
    result = lint_ami.data_folder_has_no_uncompressed_formats(good_package)

    assert result


def test_data_has_acceptable_uncompressed(good_package: Path):
    """Return false when data folder has uncompressed formats (wav, mov)"""
    (good_package / "data" / "Mezzanine").mkdir()
    (good_package / "data" / "Mezzanine" / "test_mz.mov").touch()

    result = lint_ami.data_folder_has_no_uncompressed_formats(good_package)

    assert result


def test_data_has_uncompressed(good_package: Path):
    """Return false when data folder has uncompressed formats (wav, mov)"""
    bad_package = good_package
    (bad_package / "data" / "PreservationMasters" / "bad.mov").touch()

    result = lint_ami.data_folder_has_no_uncompressed_formats(good_package)

    assert not result


def test_data_has_no_partfiles(good_package: Path):
    """Return True when data folder has no part files, e.g. ...p01_pm.flac"""
    result = lint_ami.data_folder_has_no_part_files(good_package)

    assert result


def test_data_has_partfiles(good_package: Path):
    """Return false when data folder has part files, e.g. ...p01_pm.flac"""
    bad_package = good_package
    (bad_package / "data" / "PreservationMasters" / "bad_v01p01_pm.mkv").touch()

    result = lint_ami.data_folder_has_no_part_files(good_package)

    assert not result

def test_data_subfolders_have_file(good_package: Path):
    """Return True when data subfolder has one or more files"""
    result = lint_ami.data_folders_have_at_least_two_files(good_package)

    assert result


def test_data_subfolders_have_no_file(good_package: Path):
    """Test that package fails function when there is no file at all
    within the second-level objects folder"""
    bad_package = good_package
    obj_filepaths = [x for x in bad_package.glob("data/*/*json") if x.is_file()]
    for file in obj_filepaths:
        file.unlink()
    result = lint_ami.data_folders_have_at_least_two_files(bad_package)

    assert not result


def test_package_has_no_hidden_file(good_package: Path):
    """The package should not have any hidden file"""
    result = lint_ami.package_has_no_hidden_file(good_package)

    assert result


def test_package_is_a_bag(good_package: Path):
    """Return true if package is a bag"""
    result = lint_ami.package_is_a_bag(good_package)

    assert result


def test_package_is_not_a_bag(good_package: Path):
    """Return false if package is not a bag"""
    bad_package = good_package
    (bad_package / "bagit.txt").unlink()

    result = lint_ami.package_is_a_bag(bad_package)

    assert not result


def test_package_has_hidden_file(good_package: Path):
    """Test that package fails function when there is any hidden file"""
    bad_package = good_package
    folder = bad_package / "data" / "folder"
    folder.mkdir()
    hidden_file = folder.joinpath(".DS_Store")
    hidden_file.touch()

    result = lint_ami.package_has_no_hidden_file(bad_package)

    assert not result


def test_package_has_no_zero_bytes_file(good_package: Path):
    """The package should not have any zero bytes file"""
    result = lint_ami.package_has_no_zero_bytes_file(good_package)

    assert result


def test_package_has_zero_bytes_file(good_package: Path):
    """Test that package fails function when there is any zero bytes file"""
    bad_package = good_package
    zero_bytes = bad_package / "data" / "zerobytes.txt"
    zero_bytes.touch()

    result = lint_ami.package_has_no_zero_bytes_file(bad_package)

    assert not result


def test_valid_package(good_package: Path):
    """Test that package returns 'valid' when all tests are passed"""
    result = lint_ami.lint_package(good_package)

    assert result == "valid"


def test_invalid_package(good_package: Path):
    """Test that package returns 'invalid' when all tests are passed"""
    bad_package = good_package

    bag_folder = bad_package / "data" / "ArchiveOriginals"
    bag_folder.mkdir()
    bag_file = bag_folder.joinpath("bagit.txt")
    bag_file.touch()

    result = lint_ami.lint_package(bad_package)

    assert result == "invalid"


def test_unclear_package(good_package: Path):
    """Test that package returns 'needs review' when all tests are passed"""
    bad_package = good_package
    for i in range(4):
        bad_package.joinpath(f"tags/test{i}.mkv.xml.gz").write_text("a")

    result = lint_ami.lint_package(bad_package)

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

    lint_ami.main()

    stdout = capsys.readouterr().out

    assert f"packages are valid: {[str(good_package.name)]}" in stdout


def test_lint_invalid_package(monkeypatch, good_package, capsys, tmp_path):
    """Run entire script with invalid ER"""

    bad_package = good_package

    bag_folder = bad_package / "data" / "ArchiveOriginals"
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

    lint_ami.main()

    stdout = capsys.readouterr().out

    assert f"packages are invalid: {[str(bad_package.name)]}" in stdout
