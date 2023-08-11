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


def test_extant_dir_rejects_nonexistant_dir(nonexistant_dir: Path, capsys: pytest.CaptureFixture):
    with pytest.raises(argparse.ArgumentTypeError) as exc_info:
        prsvcli.extant_dir(nonexistant_dir)

    assert f"{str(nonexistant_dir)} is not a directory" in exc_info.value.args[0]


def test_extant_dir_rejects_file(directory_of_packages: Path, capsys: pytest.CaptureFixture):
    emptyfile = directory_of_packages / "file"
    emptyfile.touch()

    with pytest.raises(argparse.ArgumentTypeError) as exc_info:
        prsvcli.extant_dir(emptyfile)

    assert f"{str(emptyfile)} is not a directory" in exc_info.value.args[0]


def test_list_of_paths_rejects_nonexistant_dir(nonexistant_dir: Path, capsys: pytest.CaptureFixture):
    with pytest.raises(argparse.ArgumentTypeError) as exc_info:
        prsvcli.list_of_paths(nonexistant_dir)

    assert f"{str(nonexistant_dir)} is not a directory" in exc_info.value.args[0]


def test_list_of_paths_rejects_childless_dir(directory_of_packages: Path, capsys: pytest.CaptureFixture):
    empty_directory = directory_of_packages / "one"

    with pytest.raises(argparse.ArgumentTypeError) as exc_info:
        prsvcli.list_of_paths(empty_directory)

    assert f"{str(empty_directory)} does not contain child directories" in exc_info.value.args[0]


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

    assert isinstance(args.packages, list)
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
        "sys.argv",
        ["script", "--directory", str(directory_of_packages)],
    )

    args = fake_cli.parse_args()

    assert isinstance(args.packages, list)
    for package in packages:
        assert package in args.packages


def test_reject_nonexistant_dirofpackages(
    nonexistant_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_packagedirectory()

    monkeypatch.setattr(
        "sys.argv", ["script", "--directory", str(nonexistant_dir)]
    )

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
        ["script", "--package", str(tmp_path), "--directory", str(directory_of_packages)],
    )

    args = fake_cli.parse_args()

    assert isinstance(args.packages, list)
    for package in packages:
        assert package in args.packages


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
