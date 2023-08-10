import sys
from pathlib import Path

import nox
from nox_poetry import Session

python_versions = ["3.11", "3.10"]
nox.options.sessions = "format", "lint", "tests"


@nox.session(python=python_versions[0])
def format(session):
    args = session.posargs or ["src", "tests"]
    session.install("black", "isort")
    session.run("black", *args)
    session.run("isort", *args)


@nox.session(python=python_versions)
def lint(session):
    args = session.posargs or ["src", "tests", "noxfile.py"]
    session.install("flake8", "flake8-bugbear")
    session.run("flake8", *args)


nox.session(python=python_versions)
def tests(session: Session) -> None:
    """Run the test suite."""
    session.install(".")
    session.install("coverage[toml]", "pytest", "pygments")
    try:
        session.run("coverage", "run", "--parallel", "-m", "pytest", *session.posargs)
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@nox.session(python=python_versions)
def mypy(session: Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or ["src", "tests"]
    session.install(".")
    session.install("mypy", "pytest")
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@nox.session(python=python_versions[0])
def coverage(session: Session) -> None:
    """Produce the coverage report."""
    args = session.posargs or ["report"]

    session.install("coverage[toml]")

    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)
