from pathlib import Path
import argparse

def parse_args() -> argparse.Namespace:
    """Validate and return command-line args"""

    def list_of_paths(p):
        path = Path(p)
        child_dirs = []
        for child in path.iterdir():
            if child.is_dir():
                child_dirs.append(child)
        return child_dirs

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--package',
        type=Path,
        nargs='+',
        dest='packages',
        action='extend'
    )
    parser.add_argument(
        '--directory',
        type=list_of_paths,
        dest='packages',
        action='extend'
    )

    return parser.parse_args()

def lint_package() -> bool:
    """Run all linting tests against a package"""
    return True

def main():
    args = parse_args()
    print(args.packages)

    if not lint_package():
        print('package did not lint')

    return False

if __name__=='__main__':
    main()
