import nox

python_versions=["3.10", "3.11"]
nox.options.sessions = "lint", "tests"

@nox.session(python=python_versions)
def tests(session):
    session.run("poetry", "install", external=True)
    session.run("pytest")

locations = "src", "tests", "noxfile.py"
@nox.session(python=python_versions)
def lint(session):
    args = session.posargs or locations
    session.install("flake8")
    session.run("flake8", *args)
