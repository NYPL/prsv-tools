import argparse
from pathlib import Path


class Parser(argparse.ArgumentParser):
    def add_instance(self) -> None:
        self.add_argument(
            "--instance",
            "-i",
            type=str,
            required=True,
            choices=["test", "prod"],
            help="Which Preservica instance to access",
        )

    def add_package(self) -> None:
        self.add_argument(
            "--package",
            type=extant_dir,
            nargs="+",
            dest="packages",
            action="extend",
            help="path to a single package",
        )

    def add_packagedirectory(self) -> None:
        self.add_argument(
            "--directory",
            type=list_of_paths,
            dest="packages",
            action="extend",
            help="path to a directory of packages",
        )

    def add_logdirectory(self, dir: Path = ".") -> None:
        self.add_argument(
            "--log_folder",
            type=extant_dir,
            help="""Optional. Designate where to save the log file,
            or it will be saved in current directory""",
            default=dir,
        )


def list_of_paths(p: str) -> list[Path]:
    path = extant_dir(p)
    child_dirs = []
    for child in path.iterdir():
        if child.is_dir():
            child_dirs.append(child)

    if not child_dirs:
        raise argparse.ArgumentTypeError(f"{path} does not contain child directories")

    return child_dirs


def extant_dir(p: str) -> Path:
    path = Path(p)
    if not path.is_dir():
        raise argparse.ArgumentTypeError(f"{path} does not exist")

    return path
