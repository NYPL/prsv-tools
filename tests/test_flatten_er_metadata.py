import bin.flatten_er_metadata_folder as flatten_md

import pytest
from pathlib import Path
import shutil

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
            '../bin/flatten_er_metadata_folder.py',
            '--package', str(child1), str(child2)
        ]
    )

    args = flatten_md.parse_args()

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
            '../bin/flatten_er_metadata_folder.py',
            '--directory', str(tmp_path)
        ]
    )

    args = flatten_md.parse_args()

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
            '../bin/flatten_er_metadata_folder.py',
            '--package', str(child1),
            '--directory', str(child2)
        ]
    )

    args = flatten_md.parse_args()

    assert child1 in args.packages
    assert grandchild in args.packages

def test_nonexistent_package(monkeypatch, tmp_path: Path, capsys):
    """Test that error is thrown if package doesn't exist"""
    child = tmp_path.joinpath('one')

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/flatten_er_metadata_folder.py',
            '--package', str(child)
        ]
    )

    with pytest.raises(SystemExit):
        flatten_md.parse_args()

    stderr = capsys.readouterr().err

    assert f'{child} does not exist' in stderr

def test_nonexistent_directory(monkeypatch, tmp_path: Path, capsys):
    """Test that error is thrown if directory doesn't exist"""
    child = tmp_path.joinpath('one')

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/flatten_er_metadata_folder.py',
            '--directory', str(child)
        ]
    )

    with pytest.raises(SystemExit):
        flatten_md.parse_args()

    stderr = capsys.readouterr().err

    assert f'{child} does not exist' in stderr

@pytest.fixture
def common_package(tmp_path: Path):
    pkg = tmp_path.joinpath('M12345_ER_0001')
    f_object = pkg.joinpath('objects')
    f_object.mkdir(parents=True)
    f_metadata = pkg.joinpath('metadata')
    f_metadata.mkdir()

    f_submission = f_metadata.joinpath('submissionDocumentation')
    f_submission.mkdir()
    metadata_filepath = f_submission.joinpath('M12345_ER_0001.csv')
    metadata_filepath.touch()
    metadata_filepath.write_bytes(b'some bytes for metadata')

    return pkg

def test_get_submissionDocumentation_path(common_package):
    """Test that get_submissionDocumentation_path function returns a Path object
    when submissionDocumentation folder exists in the metadata folder"""
    subdoc_path = flatten_md.get_submissionDocumentation_path(common_package)
    result = isinstance(subdoc_path, Path)
    # The isinstance() function returns whether the specified object is of the specified type.

    assert result == True

def test_nonexistent_subdoc_path(common_package):
    """Test that get_submissionDocumentation_path returns None when the submissionDocumentation
    folder does not exist"""
    bad_package = common_package

    for subdoc in bad_package.rglob('submissionDocumentation'):
        shutil.rmtree(subdoc)

    subdoc_path = flatten_md.get_submissionDocumentation_path(common_package)
    assert subdoc_path == None
# Functional tests