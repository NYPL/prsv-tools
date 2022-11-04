import bin.lint_er as lint_er

from pathlib import Path


# Unit tests
## Argument tests
def test_accepts_paths(monkeypatch):
    """Test that packages are returned as a list of pathlib paths"""
    paths = ['firstPath', 'secondPath']

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--package', paths[0], paths[1]
        ]
    )

    args = lint_er.parse_args()
    for x in paths:
        assert Path(x) in args.packages


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

