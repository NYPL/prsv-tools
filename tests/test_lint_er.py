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
def top_folder(tmp_path: Path):
    top_f = tmp_path.joinpath('M12345_ER_0001')
    top_f.mkdir()
    
    return top_f

@pytest.fixture
def metadata_folder(top_folder: Path):
    md_f = top_folder.joinpath('metadata')
    md_f.mkdir()
    md_filepath = md_f.joinpath('M12345_ER_0001.csv')
    md_filepath.touch()
    md_filepath.write_bytes(b'some bytes for metadata')
    
    return md_f

@pytest.fixture
def objects_folder(top_folder: Path):
    obj_f = top_folder.joinpath('objects')
    obj_f.mkdir()
    obj_filepath = obj_f.joinpath('randomFile.txt')
    obj_filepath.touch()
    obj_filepath.write_bytes(b'some bytes for object')
    
    return obj_f

@pytest.fixture
def good_package(top_folder, metadata_folder, objects_folder):
    top_folder
    metadata_folder
    objects_folder

    return top_folder

def test_top_folder_valid_name(top_folder):
    """Top level folder name has to conform to M###_(ER|DI|EM)_####"""
    result = lint_er.package_has_valid_name(top_folder)

    assert result == True

def test_top_folder_invalid_name(top_folder):
    """Negative test for top level folder naming convention"""
    wrong_name = top_folder.rename('M12345')
    
    result = lint_er.package_has_valid_name(wrong_name)

    assert result == False


def test_sec_level_folder_valid_names(good_package):
    """Second level folders must have objects and metadata folder"""
    result = lint_er.package_has_valid_subfolder_names(good_package)

    assert result == True

def test_sec_level_folder_invalid_names(good_package):
    """Negative test for second level folders structure, objects and metadata folders"""
    bad_package = good_package
    for objects_path in bad_package.glob('objects'):
        objects_path.rename(bad_package / 'obj')

    result = lint_er.package_has_valid_subfolder_names(bad_package)

    assert result == False

def test_objects_folder_has_no_access_folder(good_package):
    """An access folder within the objects folder indicates it is an older package,
    and the files within the access folder was created by the Library, and should not be ingested"""
    result = lint_er.objects_folder_has_no_access_folder(good_package)

    assert result == True

def test_objects_folder_has_access_folder(good_package):
    """Negative test for access folder in objects folder"""
    bad_package = good_package
    for objects_path in bad_package.glob('objects'):
        access_dir = objects_path.joinpath('access')
        access_dir.mkdir()

    result = lint_er.objects_folder_has_no_access_folder(bad_package)

    assert result == False

def test_metadata_folder_is_flat(good_package):
    """The metadata folder should not have folder structure"""
    result = lint_er.metadata_folder_is_flat(good_package)

    assert result == True

def test_metadata_folder_has_random_folder(good_package):
    """Negative test for metadata_folder_is_flat"""
    bad_package = good_package
    for metadata_path in bad_package.glob('metadata'):
        random_dir = metadata_path.joinpath('random_dir')
        random_dir.mkdir()

    result = lint_er.metadata_folder_is_flat(bad_package)

    assert result == False

def test_metadata_folder_has_submissionDocumentation_folder(good_package):
    """Negative test for metadata_folder_is_flat"""
    bad_package = good_package
    for metadata_path in bad_package.glob('metadata'):
        random_dir = metadata_path.joinpath('submissionDocumentation')
        random_dir.mkdir()

    result = lint_er.metadata_folder_is_flat(bad_package)

    assert result == False

def test_metadata_folder_has_one_or_less_file(good_package):
    """metadata folder should have zero to one file"""
    result = lint_er.metadata_folder_has_one_or_less_file(good_package)

    assert result == True

def test_metadata_folder_has_more_than_one_file(good_package):
    """Negative test for metadata_folder_has_one_or_less_file"""
    bad_package = good_package
    for metadata_path in bad_package.glob('metadata'):
        new_md_file = metadata_path.joinpath('M12345_ER_0002.csv')
        new_md_file.touch()

    result = lint_er.metadata_folder_has_one_or_less_file(bad_package)

    assert result == False

def test_metadata_file_valid_name(good_package):
    """FTK metadata CSV name should conform to M###_(ER|DI|EM)_####.(csv|CSV)"""
    result = lint_er.metadata_file_has_valid_filename(good_package)

    assert result == True

def test_metadata_file_invalid_name_tsv(good_package):
    """Negative test for metadata_file_valid_name"""
    bad_package = good_package
    for metadata_path in bad_package.glob('metadata'):
        for file in [x for x in metadata_path.iterdir() if x.is_file()]:
            file.rename(metadata_path / 'M12345_ER_0001.tsv')
    
    result = lint_er.metadata_file_has_valid_filename(bad_package)

    assert result == False

def test_metadata_file_invalid_name_more_files(good_package):
    """Negative test for metadata_file_valid_name"""
    bad_package = good_package
    for metadata_path in bad_package.glob('metadata'):
        new_csv = metadata_path.joinpath('M12345_ER_0003.csv')
        new_csv.touch()
        new_tsv = metadata_path.joinpath('M12345_ER_0004.tsv')
        new_tsv.touch()
        random_file = metadata_path.joinpath('M1234.txt')
        random_file.touch()
    
    result = lint_er.metadata_file_has_valid_filename(bad_package)

    assert result == False

def test_objects_folder_has_file(good_package):
    """The objects folder must have one or more files, which can be in folder(s)"""
    result = lint_er.objects_folder_has_file(good_package)

    assert result == True

def test_objects_folder_has_no_file(good_package):
    """Negative test for objects_folder_has_file"""
    bad_package = good_package
    obj_filepaths = [x for x in bad_package.glob('objects/*') if x.is_file()]
    for file in obj_filepaths:
        file.unlink()
    result = lint_er.objects_folder_has_file(bad_package)

    assert result == False

def test_package_has_no_bag(good_package):
    """The package should not have bag structures"""
    result = lint_er.package_has_no_bag(good_package)

    assert result == True

def test_package_has_no_hidden_file(good_package):
    """The package should not have any hidden file"""
    result = lint_er.package_has_no_hidden_file(good_package)

    assert result == True

def test_package_has_hidden_file(good_package):
    """Negative test for package_has_no_hidden_file"""
    bad_package = good_package
    for objects_path in bad_package.glob('objects'):
        folder = objects_path.joinpath('folder')
        folder.mkdir()
        hidden_file = folder.joinpath('.DS_Store')
        hidden_file.touch()

    result = lint_er.package_has_no_hidden_file(bad_package)

    assert result == False

def test_package_has_no_zero_bytes_file(good_package):
    """The package should not have any zero bytes file"""
    result = lint_er.package_has_no_zero_bytes_file(good_package)

    assert result == True

def test_package_has_zero_bytes_file(good_package):
    """Negative test for package_has_no_zero_bytes_file"""
    bad_package = good_package
    for objects_path in bad_package.glob('objects'):
        zero_bytes = objects_path.joinpath('zerobytes.txt')
        zero_bytes.touch()

    result = lint_er.package_has_no_zero_bytes_file(bad_package)

    assert result == False

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

