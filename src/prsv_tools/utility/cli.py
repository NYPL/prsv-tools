import argparse
from pathlib import Path

class Parser(argparse.ArgumentParser):

    def list_of_paths(self, p):
            path = self.extant_dir(p)
            child_dirs = []
            for child in path.iterdir():
                if child.is_dir():
                    child_dirs.append(child)
            return child_dirs


    def extant_dir(self, p):
            path = Path(p)
            if not path.is_dir():
                raise argparse.ArgumentTypeError(f"{path} does not exist")

            return path


    def add_instance(self):
        self.add_argument(
            "--instance",
            "-i",
            type=str,
            required=True,
            choices=["test", "prod"],
            help="Which Preservica instance to access",
        )


    def add_package(self):
        self.add_argument(
            "--package", type=self.extant_dir, nargs="+", dest="packages", action="extend"
        )


    def add_packagedirectory(self):
        self.add_argument(
            "--directory", type=self.list_of_paths, dest="packages", action="extend"
        )
