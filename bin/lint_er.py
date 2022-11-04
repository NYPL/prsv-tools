from pathlib import Path
import argparse

def parse_args():
    """Validate and return command-line args"""
    parser = argparse.ArgumentParser()

    return parser.parse_args()

def lint_package() -> bool:
    """Run all linting tests against a package"""
    return True

def main():
    args = parse_args()

    if not lint_package():
        print('package did not lint')

    return False

if __name__=='__main__':
    main()
