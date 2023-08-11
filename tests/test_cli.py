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
    directory_of_packages: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
):
    fake_cli = prsvcli.Parser()
    fake_cli.add_package()

    invalid_package = directory_of_packages / "nonexistant"

    monkeypatch.setattr("sys.argv", ["script", "--package", str(invalid_package)])

    with pytest.raises(SystemExit):
        fake_cli.parse_args()

    stderr = capsys.readouterr().err

    assert f"{str(invalid_package)} does not exist" in stderr


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
