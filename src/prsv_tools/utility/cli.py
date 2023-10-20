import argparse
import re
import sys
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
            action=ExtendUnique,
            help="path to a single package",
        )

    def add_packagedirectory(self) -> None:
        self.add_argument(
            "--directory",
            type=list_of_paths,
            dest="packages",
            action=ExtendUnique,
            help="path to a directory of packages",
        )

    CWD = Path(".")

    def add_logdirectory(self, dir: Path = CWD) -> None:
        self.add_argument(
            "--log_folder",
            type=extant_dir,
            help="""Optional. Designate where to save the log file,
            or it will be saved in current directory""",
            default=dir,
        )

    def add_id_search(self):
        ids = self.add_argument_group(
            description="IDs that can be searched. At least 1 ID is required"
        )

        ids.add_argument(
            "--coll",
            required="--er" in sys.argv or ("--ami" in sys.argv and "all" in sys.argv),
            type=coll_id,
            help="collection ID, M followed by 3-7 digits (M1234567)",
        )

        ids.add_argument(
            "--object",
            required=False,
            type=object_id,
            nargs="+",
            help='SPEC object ID, 6-7 digits (1234567), or "all"',
        )
        ids.add_argument(
            "--er",
            required=False,
            type=er_id,
            nargs="+",
            help='electronic record number, ER, DI, or EM followed by 1-4 digits (ER 123), requires a collection, or "all"',
        )
        ids.add_argument(
            "--ami",
            required=False,
            type=ami_id,
            nargs="+",
            help='AMI ID, 6 digits (123456), or "all"',
        )
        ids.add_argument(
            "--acq",
            required="--object" in sys.argv and "all" in sys.argv,
            help="acquisition ID, unknown",
        )

        # it would be better to extend the parse_args function, but have not
        # figured out how
        all_ids = [item.dest for item in ids._actions]
        if not any(f"--{id}" in sys.argv for id in all_ids):
            self.error("at least one ID argument is required")


class ExtendUnique(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        items = getattr(namespace, self.dest, None)

        if items is None:
            items = set(values)
        elif isinstance(items, set):
            items = items.union(values)

        setattr(namespace, self.dest, items)


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
        raise argparse.ArgumentTypeError(f"{path} is not a directory")

    return path


def is_valid_id(type: str, pattern: str, id: str) -> str:
    if not re.match(pattern, id):
        raise argparse.ArgumentTypeError(
            f"{id} does not match the expected {type} pattern, {pattern}"
        )
    return id


def coll_id(id: str) -> str:
    return is_valid_id("collection ID", r"(M|L)\d\d\d+$", id)


def object_id(id: str) -> str:
    return is_valid_id("SPEC object ID", r"(\d{6,7}$|all)", id)


def er_id(id: str) -> str:
    return is_valid_id("electronic record number", r"((ER|DI|EM)_\d+$|all)", id)


def ami_id(id: str) -> str:
    return is_valid_id("AMI ID", r"(\d{6}$|all)", id)
