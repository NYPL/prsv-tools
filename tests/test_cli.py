import argparse
from pathlib import Path

import pytest

import prsv_tools.utility.cli as prsvcli


@pytest.fixture
def directory_of_packages(tmp_path: Path):
    child1 = tmp_path / "one"
    child1.mkdir()
    child2 = tmp_path / "two"
    child2.mkdir()

    return tmp_path


@pytest.fixture
def nonexistant_dir(tmp_path: Path):
    return tmp_path / "nonexistant"


def test_extant_dir_rejects_nonexistant_dir(
    nonexistant_dir: Path, capsys: pytest.CaptureFixture
):
    with pytest.raises(argparse.ArgumentTypeError) as exc_info:
        prsvcli.extant_dir(nonexistant_dir)

    assert f"{str(nonexistant_dir)} is not a directory" in exc_info.value.args[0]


def test_extant_dir_rejects_file(
    directory_of_packages: Path, capsys: pytest.CaptureFixture
):
    emptyfile = directory_of_packages / "file"
    emptyfile.touch()

    with pytest.raises(argparse.ArgumentTypeError) as exc_info:
        prsvcli.extant_dir(emptyfile)

    assert f"{str(emptyfile)} is not a directory" in exc_info.value.args[0]


def test_list_of_paths_rejects_nonexistant_dir(
    nonexistant_dir: Path, capsys: pytest.CaptureFixture
):
    with pytest.raises(argparse.ArgumentTypeError) as exc_info:
        prsvcli.list_of_paths(nonexistant_dir)

    assert f"{str(nonexistant_dir)} is not a directory" in exc_info.value.args[0]


def test_list_of_paths_rejects_childless_dir(
    directory_of_packages: Path, capsys: pytest.CaptureFixture
):
    empty_directory = directory_of_packages / "one"

    with pytest.raises(argparse.ArgumentTypeError) as exc_info:
        prsvcli.list_of_paths(empty_directory)

    assert (
        f"{str(empty_directory)} does not contain child directories"
        in exc_info.value.args[0]
    )


def test_accept_valid_package(
    directory_of_packages: Path, monkeypatch: pytest.MonkeyPatch
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_package()

    packages = [path for path in directory_of_packages.iterdir()]

    monkeypatch.setattr(
        "sys.argv",
        ["script", "--package", str(packages[0]), "--package", str(packages[1])],
    )

    args = fake_cli.parse_args()

    for package in packages:
        assert package in args.packages


def test_reject_invalid_package(
    nonexistant_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_package()

    monkeypatch.setattr("sys.argv", ["script", "--package", str(nonexistant_dir)])

    with pytest.raises(SystemExit):
        fake_cli.parse_args()

    stderr = capsys.readouterr().err

    assert f"{str(nonexistant_dir)} is not a directory" in stderr


def test_accept_valid_dirofpackages(
    directory_of_packages: Path, monkeypatch: pytest.MonkeyPatch
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_packagedirectory()

    packages = [path for path in directory_of_packages.iterdir()]

    monkeypatch.setattr(
        "sys.argv", ["script", "--directory", str(directory_of_packages)]
    )

    args = fake_cli.parse_args()

    for package in packages:
        assert package in args.packages


def test_reject_nonexistant_dirofpackages(
    nonexistant_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_packagedirectory()

    monkeypatch.setattr("sys.argv", ["script", "--directory", str(nonexistant_dir)])

    with pytest.raises(SystemExit):
        fake_cli.parse_args()

    stderr = capsys.readouterr().err

    assert f"{str(nonexistant_dir)} is not a directory" in stderr


def test_reject_empty_dirofpackages(
    directory_of_packages: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_packagedirectory()

    subdir = next(directory_of_packages.iterdir())

    monkeypatch.setattr("sys.argv", ["script", "--directory", str(subdir)])

    with pytest.raises(SystemExit):
        fake_cli.parse_args()

    stderr = capsys.readouterr().err

    assert f"{str(subdir)} does not contain child directories" in stderr


def test_accept_package_and_directory_of_packages(
    tmp_path: Path, directory_of_packages: Path, monkeypatch: pytest.MonkeyPatch
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_package()
    fake_cli.add_packagedirectory()

    packages = [path for path in directory_of_packages.iterdir()]
    packages.append(tmp_path)

    monkeypatch.setattr(
        "sys.argv",
        [
            "script",
            "--package",
            str(tmp_path),
            "--directory",
            str(directory_of_packages),
        ],
    )

    args = fake_cli.parse_args()

    for package in packages:
        assert package in args.packages


def test_remove_repeated_directory_paths(
    directory_of_packages: Path, monkeypatch: pytest.MonkeyPatch
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_packagedirectory()

    packages = [path for path in directory_of_packages.iterdir()]

    monkeypatch.setattr(
        "sys.argv",
        [
            "script",
            "--directory",
            str(directory_of_packages),
            "--directory",
            str(directory_of_packages),
        ],
    )

    args = fake_cli.parse_args()
    assert len(args.packages) == len(packages)


@pytest.mark.parametrize("instance", ["test", "prod"])
def test_accept_valid_instance(monkeypatch: pytest.MonkeyPatch, instance: str):
    fake_cli = prsvcli.Parser()
    fake_cli.add_instance()

    monkeypatch.setattr("sys.argv", ["script", "--instance", instance])

    args = fake_cli.parse_args()

    assert instance in args.instance


def test_reject_invalid_instance(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_instance()

    instance = "nonexistant"
    monkeypatch.setattr("sys.argv", ["script", "--instance", instance])

    with pytest.raises(SystemExit):
        fake_cli.parse_args()

    stderr = capsys.readouterr().err

    assert f"invalid choice: '{instance}'" in stderr


def test_default_loglocation(monkeypatch: pytest.MonkeyPatch):
    fake_cli = prsvcli.Parser()
    fake_cli.add_logdirectory()

    monkeypatch.setattr("sys.argv", ["script"])

    args = fake_cli.parse_args()

    assert args.log_folder == Path(".")


def test_accept_valid_loglocation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    fake_cli = prsvcli.Parser()
    fake_cli.add_logdirectory()

    monkeypatch.setattr("sys.argv", ["script", "--log_folder", str(tmp_path)])

    args = fake_cli.parse_args()

    assert args.log_folder == tmp_path


def test_reject_invalid_logdirectory(
    nonexistant_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_logdirectory()

    monkeypatch.setattr("sys.argv", ["script", "--log_folder", str(nonexistant_dir)])

    with pytest.raises(SystemExit):
        fake_cli.parse_args()

    stderr = capsys.readouterr().err

    assert f"{str(nonexistant_dir)} is not a directory" in stderr


# All tests for search related arguments
SINGLE_ID_ARGS = {
    'one_ami': {
        '--ami': '123456'
    },
    'one_spec_object': {
        '--object': '123456'
    }
}

SET_IDS = ['--coll', '--acq']

SET_ID_ARGS = {
    'one_er': {
        '--coll': 'M12345',
        '--er': 'ER_1'
    },
    'all_coll_ami': {
        '--coll': 'M12345',
        '--ami': 'all'
    },
    'all_coll_er': {
        '--er': 'all',
        '--coll': 'M12345'
    },
    'all_coll': {
        '--coll': 'M12345',
        '--er': 'all',
        '--ami': 'all'
    },
    'all_acq': {
        '--object': 'all',
        '--acq': '123456'
    }
}

ALL_ID_ARGS = {**SINGLE_ID_ARGS, **SET_ID_ARGS}


@pytest.mark.parametrize("scenario,ids", ALL_ID_ARGS.items())
def test_accept_valid_id_sets(monkeypatch: pytest.MonkeyPatch, scenario: str, ids: dict):
    cmd = ['script']
    for id_type, id_value in ids.items():
        cmd.extend([id_type, id_value])

    monkeypatch.setattr("sys.argv", cmd)

    fake_cli = prsvcli.Parser()
    fake_cli.add_id_search()

    args = fake_cli.parse_args()

    for id_type, id_value in ids.items():
        assert id_value in getattr(args, id_type[2:])


@pytest.mark.parametrize("scenario,ids", ALL_ID_ARGS.items())
def test_reject_invalid_ids(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, scenario: str, ids: dict):
    cmd = ['script']

    first_value = None
    for id_type, id_value in ids.items():
        if not first_value:
            first_value = id_value
        cmd.extend([id_type, f'@{id_value}@'])

    monkeypatch.setattr('sys.argv', cmd)

    fake_cli = prsvcli.Parser()
    fake_cli.add_id_search()

    with pytest.raises(SystemExit):
        fake_cli.parse_args()

    stderr = capsys.readouterr().err

    assert f'@{first_value}@ does not match the expected' in stderr


@pytest.mark.parametrize("scenario,ids", SET_ID_ARGS.items())
def test_reject_missing_ids(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, scenario: str, ids: dict):
    cmd = ['script']

    for id_type, id_value in ids.items():
        if not id_type in SET_IDS:
            cmd.extend([id_type, id_value])
        else:
            required = id_type

    monkeypatch.setattr('sys.argv', cmd)

    fake_cli = prsvcli.Parser()
    fake_cli.add_id_search()

    with pytest.raises(SystemExit):
        fake_cli.parse_args()

    stderr = capsys.readouterr().err

    assert f'the following arguments are required: {required}' in stderr


@pytest.mark.parametrize("scenario,ids", ALL_ID_ARGS.items())
def test_accept_multiple_ids(monkeypatch: pytest.MonkeyPatch, scenario: str, ids: dict):
    cmd = ['script']
    number_of_id_args = 6

    for id_type, id_value in ids.items():
        cmd.append(id_type)
        if not id_type in SET_IDS:
            cmd.extend([id_value] * number_of_id_args)
            final_type = id_type
            print(id_type)
        else:
            cmd.append(id_value)

    monkeypatch.setattr('sys.argv', cmd)

    fake_cli = prsvcli.Parser()
    fake_cli.add_id_search()

    args = fake_cli.parse_args()

    assert number_of_id_args == len(getattr(args, final_type[2:]))

def test_require_at_least_one_id(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture):
    cmd = ['script']

    monkeypatch.setattr('sys.argv', cmd)

    fake_cli = prsvcli.Parser()

    with pytest.raises(SystemExit):
        fake_cli.add_id_search()

    stderr = capsys.readouterr().err

    assert f'at least one ID argument is required' in stderr
