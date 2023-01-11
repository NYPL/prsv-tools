import bin.lint_er as lint_er

import pytest
from pathlib import Path


# Unit tests
## Argument tests
def test_accepts_paths(monkeypatch, tmp_path: Path):
    """Test that packages are returned as a list of pathlib paths"""
    child1 = tmp_path.joinpath('one')
    child1.mkdir()
    child2 = tmp_path.joinpath('two')
    child2.mkdir()

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--package', str(child1), str(child2)
        ]
    )

    args = lint_er.parse_args()

    assert child1 in args.packages
    assert child2 in args.packages

def test_accepts_dir_of_packages(monkeypatch, tmp_path: Path):
    """Test that a directory returns a list of child paths"""
    child1 = tmp_path.joinpath('one')
    child1.mkdir()
    child2 = tmp_path.joinpath('two')
    child2.mkdir()

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--directory', str(tmp_path)
        ]
    )

    args = lint_er.parse_args()

    assert child1 in args.packages
    assert child2 in args.packages

def test_accept_package_and_dir(monkeypatch, tmp_path: Path):
    child1 = tmp_path.joinpath('one')
    child1.mkdir()
    child2 = tmp_path.joinpath('two')
    child2.mkdir()
    grandchild = child2.joinpath('2.4')
    grandchild.mkdir()

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--package', str(child1),
            '--directory', str(child2)
        ]
    )

    args = lint_er.parse_args()

    assert child1 in args.packages
    assert grandchild in args.packages

def test_nonexistent_package(monkeypatch, tmp_path: Path, capsys):
    """Test that error is thrown if package doesn't exist"""
    child = tmp_path.joinpath('one')

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--package', str(child)
        ]
    )

    with pytest.raises(SystemExit):
        lint_er.parse_args()

    stderr = capsys.readouterr().err

    assert f'{child} does not exist' in stderr

def test_nonexistent_directory(monkeypatch, tmp_path: Path, capsys):
    """Test that error is thrown if directory doesn't exist"""
    child = tmp_path.joinpath('one')

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--directory', str(child)
        ]
    )

    with pytest.raises(SystemExit):
        lint_er.parse_args()

    stderr = capsys.readouterr().err

    assert f'{child} does not exist' in stderr

@pytest.fixture
def good_package(tmp_path: Path):
    pkg = tmp_path.joinpath('M12345_ER_0001')
    f_object = pkg.joinpath('objects')
    f_object.mkdir(parents=True)
    object_filepath = f_object.joinpath('randomFile.txt')
    object_filepath.touch()

    f_metadata = pkg.joinpath('metadata')
    f_metadata.mkdir()

    metadata_filepath = f_metadata.joinpath('M12345_ER_0001.csv')
    metadata_filepath.touch()

    return pkg

def test_top_folder_valid_name(good_package):
    """Top level folder name has to conform to M###_(ER|DI|EM)_####"""
    result = lint_er.package_has_valid_name(good_package)

    assert result == True

def test_top_folder_invalid_name(tmp_path: Path):
    bad_package = tmp_path.joinpath('M1234_0001')
    result = lint_er.package_has_valid_name(bad_package)

    assert result == False


def test_sec_level_folder_valid_names(good_package):
    """Second level folders must have objects and metadata folder"""
    result = lint_er.package_has_valid_subfolder_names(good_package)

    assert result == True

def test_objects_folder_has_no_access_folder(good_package):
    """An access folder within the objects folder indicates it is an older package,
    and the files within the access folder was created by the Library, and should not be ingested"""
    result = lint_er.objects_folder_has_no_access_folder(good_package)

    assert result == True


def test_metadata_folder_is_flat(good_package):
    """The metadata folder should not have folder structure"""
    result = lint_er.metadata_folder_is_flat(good_package)

    assert result == True

def test_metadata_folder_has_one_or_less_file(good_package):
    """metadata folder should have zero to one file"""
    result = lint_er.metadata_folder_has_one_or_less_file(good_package)

    assert result == True

def test_metadata_file_valid_name(good_package):
    """FTK metadata CSV name should conform to M###_(ER|DI|EM)_####.(csv|CSV)"""
    result = lint_er.metadata_file_has_valid_filename(good_package)

    assert result == True

def test_objects_folder_has_file(good_package):
    """The objects folder must have one or more files, which can be in folder(s)"""
    result = lint_er.objects_folder_has_file(good_package)

    assert result == True

def test_package_has_no_bag(good_package):
    """The package should not have bag structures"""
    result = lint_er.package_has_no_bag(good_package)

    assert result == True

def test_package_has_no_zip(good_package):
    """The package should not have zip structures"""
    result = lint_er.package_has_no_zip(good_package)

    assert result == True

def test_package_has_no_hidden_file(good_package):
    """The package should not have any hidden file"""
    result = lint_er.package_has_no_hidden_file(good_package)

    assert result == True

# Functional tests
def test_lint_valid_package(monkeypatch, tmp_path: Path):
    """Run entire script with valid ER"""

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--package', str(tmp_path)
        ]
    )

    lint_er.main()
    assert False

def test_lint_invalid_package(monkeypatch, tmp_path: Path):
    """Run entire script with valid ER"""

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--package', str(tmp_path)
        ]
    )

    lint_er.main()
    assert False

