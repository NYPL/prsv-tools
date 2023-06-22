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
    object_filepath.write_bytes(b'some bytes for object')

    f_metadata = pkg.joinpath('metadata')
    f_metadata.mkdir()

    metadata_filepath = f_metadata.joinpath('M12345_ER_0001.csv')
    metadata_filepath.touch()
    metadata_filepath.write_bytes(b'some bytes for metadata')

    return pkg

def test_top_folder_valid_name(good_package):
    """Top level folder name has to conform to M###_(ER|DI|EM)_####"""
    result = lint_er.package_has_valid_name(good_package)

    assert result == True

def test_top_folder_invalid_name(good_package):
    """Test that package fails function when the top level folder name
    does not conform to the naming convention, M###_(ER|DI|EM)_####"""
    bad_package = good_package
    bad_package = bad_package.rename(bad_package.parent / 'M12345')

    result = lint_er.package_has_valid_name(bad_package)

    assert result == False


def test_sec_level_folder_valid_names(good_package):
    """Second level folders must only have objects and metadata folder"""
    result = lint_er.package_has_valid_subfolder_names(good_package)

    assert result == True

def test_sec_level_folder_invalid_names(good_package):
    """Test that package fails function when second level folders are not named
    objects and metadata, OR when there are more folders other than
    the objects and metadata folders."""
    bad_package = good_package
    for objects_path in bad_package.glob('objects'):
        objects_path.rename(bad_package / 'obj')

    result = lint_er.package_has_valid_subfolder_names(bad_package)

    assert result == False

def test_objects_folder_has_no_access_folder(good_package):
    """The package should not have an 'access' folder that was created by the Library.
    As the access folder and files in it were created by the Library, they should not be ingested"""
    result = lint_er.objects_folder_has_no_access_folder(good_package)

    assert result == True

def test_objects_folder_has_access_folder(good_package):
    """Test that package fails function when it includes folder(s) named access"""
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
    """Test that package fails function when the second-level metadata folder
    has any folder in it"""
    bad_package = good_package
    for metadata_path in bad_package.glob('metadata'):
        random_dir = metadata_path.joinpath('random_dir')
        random_dir.mkdir()

    result = lint_er.metadata_folder_is_flat(bad_package)

    assert result == False

def test_metadata_folder_has_submissionDocumentation_folder(good_package):
    """Test that package fails function and gives out correct error message
    when the second-level metadata folder has the submissionDocumentation folder"""
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
    """Test that package fails when there are more then one file
    in the second-level metadata folder"""
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
    """Test that package fails function and gives out correct warning
    when the metadata file name does not conform to the naming convention,
    M###_(ER|DI|EM)_####.(csv|CSV), but M###_(ER|DI|EM)_####.(tsv|TSV)"""
    bad_package = good_package
    for metadata_path in bad_package.glob('metadata'):
        for file in [x for x in metadata_path.iterdir() if x.is_file()]:
            file.rename(metadata_path / 'M12345_ER_0001.tsv')

    result = lint_er.metadata_file_has_valid_filename(bad_package)

    assert result == False

def test_metadata_file_invalid_name_more_files(good_package):
    """Test that package fails function and gives out correct warning when
    there are more than one file in the second-level metadata folder"""
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
    """Test that package fails function when there is no file at all
    within the second-level objects folder"""
    bad_package = good_package
    obj_filepaths = [x for x in bad_package.glob('objects/*') if x.is_file()]
    for file in obj_filepaths:
        file.unlink()
    result = lint_er.objects_folder_has_file(bad_package)

    assert result == False

def test_objects_folder_has_empty_folder(good_package):
    """Test that package fails function when there is no file, but an empty folder
    within the second-level objects folder"""
    bad_package = good_package
    obj_filepaths = [x for x in bad_package.glob('objects/*') if x.is_file()]
    for file in obj_filepaths:
        file.unlink()
    for objects_path in bad_package.glob('objects'):
        empty_folder = objects_path.joinpath('empty_folder')
        empty_folder.mkdir()

    result = lint_er.objects_folder_has_file(bad_package)

    assert result == False

def test_package_has_no_bag(good_package):
    """The package should not have bag structures"""
    result = lint_er.package_has_no_bag(good_package)

    assert result == True

def test_package_has_bag(good_package):
    """Test that package fails function when there is any bagit.txt file,
    indicating bag structure exists in the package"""
    bad_package = good_package
    for obj_path in bad_package.glob('objects'):
        bag_folder = obj_path.joinpath('bagfolder')
        bag_folder.mkdir()
        bag_file = bag_folder.joinpath('bagit.txt')
        bag_file.touch()

    result = lint_er.package_has_no_bag(bad_package)

    assert result == False

def test_package_has_no_hidden_file(good_package):
    """The package should not have any hidden file"""
    result = lint_er.package_has_no_hidden_file(good_package)

    assert result == True

def test_package_has_hidden_file(good_package):
    """Test that package fails function when there is any hidden file"""
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
    """Test that package fails function when there is any zero bytes file"""
    bad_package = good_package
    for objects_path in bad_package.glob('objects'):
        zero_bytes = objects_path.joinpath('zerobytes.txt')
        zero_bytes.touch()

    result = lint_er.package_has_no_zero_bytes_file(bad_package)

    assert result == False

def test_valid_package(good_package):
    """Test that package returns 'valid' when all tests are passed"""
    result = lint_er.lint_package(good_package)

    assert result == 'valid'

def test_invalid_package(good_package):
    """Test that package returns 'invalid' when all tests are passed"""
    bad_package = good_package

    objects = bad_package.joinpath('objects')
    bag_folder = objects.joinpath('bagfolder')
    bag_folder.mkdir()
    bag_file = bag_folder.joinpath('bagit.txt')
    bag_file.touch()

    result = lint_er.lint_package(bad_package)

    assert result == 'invalid'

def test_unclear_package(good_package):
    """Test that package returns 'needs review' when all tests are passed"""
    bad_package = good_package
    bad_package.joinpath('metadata').joinpath('M12345_ER_0002.csv').write_text('a')

    result = lint_er.lint_package(bad_package)

    assert result == 'needs review'

# Functional tests
def test_lint_valid_package(monkeypatch, good_package, capsys):
    """Run entire script with valid ER"""

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--package', str(good_package)
        ]
    )

    lint_er.main()

    stdout = capsys.readouterr().out

    assert f'The following packages are valid: {str(good_package)}' in stdout

def test_lint_invalid_package(monkeypatch, good_package, capsys):
    """Run entire script with invalid ER"""

    bad_package = good_package

    objects = bad_package.joinpath('objects')
    bag_folder = objects.joinpath('bagfolder')
    bag_folder.mkdir()
    bag_file = bag_folder.joinpath('bagit.txt')
    bag_file.touch()

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--package', str(bad_package)
        ]
    )

    lint_er.main()

    stdout = capsys.readouterr().out

    assert f'The following packages are invalid: {str(bad_package)}' in stdout