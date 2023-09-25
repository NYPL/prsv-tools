#!/usr/bin/env python3

from pathlib import Path

import pytest

import prsv_tools.ingest.mv_access_folder as mv_access_folder

# Unit tests
# Argument tests
def test_package_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "sys.argv",
        ["script", "--package", str(tmp_path)],
    )

    args = mv_access_folder.parse_args()

    assert tmp_path in args.packages


def test_directory_argument(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    child_dir = tmp_path / "child"
    child_dir.mkdir()

    monkeypatch.setattr(
        "sys.argv",
        ["script", "--directory", str(tmp_path)],
    )

    args = mv_access_folder.parse_args()

    assert child_dir in args.packages