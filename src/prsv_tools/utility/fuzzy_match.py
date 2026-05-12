import argparse
import json
import sys
from pathlib import Path
from typing import Any


def fuzzy_compare(obj1: Any, obj2: Any, path: str = "") -> list[str]:
    """
    Recursively compares two objects and returns a list of differences.
    Strings are compared case-insensitively.
    Dictionary key order is ignored.
    """
    diffs = []

    if type(obj1) != type(obj2):
        diffs.append(f"Type mismatch at {path or 'root'}: {type(obj1).__name__} vs {type(obj2).__name__}")
        return diffs

    if isinstance(obj1, dict):
        keys1 = set(obj1.keys())
        keys2 = set(obj2.keys())

        if keys1 != keys2:
            missing_in_2 = keys1 - keys2
            missing_in_1 = keys2 - keys1
            if missing_in_2:
                diffs.append(f"Keys missing in second file at {path or 'root'}: {missing_in_2}")
            if missing_in_1:
                diffs.append(f"Keys missing in first file at {path or 'root'}: {missing_in_1}")

        # Compare common keys
        for key in keys1 & keys2:
            new_path = f"{path}.{key}" if path else key
            diffs.extend(fuzzy_compare(obj1[key], obj2[key], new_path))

    elif isinstance(obj1, list):
        if len(obj1) != len(obj2):
            diffs.append(f"List length mismatch at {path or 'root'}: {len(obj1)} vs {len(obj2)}")
        else:
            for i, (item1, item2) in enumerate(zip(obj1, obj2)):
                new_path = f"{path}[{i}]"
                diffs.extend(fuzzy_compare(item1, item2, new_path))

    elif isinstance(obj1, str):
        if obj1.lower() != obj2.lower():
            diffs.append(f"Value mismatch at {path or 'root'}: '{obj1}' vs '{obj2}'")

    else:
        if obj1 != obj2:
            diffs.append(f"Value mismatch at {path or 'root'}: {obj1} vs {obj2}")

    return diffs


def parse_args():
    parser = argparse.ArgumentParser(description="Compare two JSON files with fuzzy matching.")
    parser.add_argument("file1", type=Path, help="Path to the first JSON file")
    parser.add_argument("file2", type=Path, help="Path to the second JSON file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print all differences")
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.file1.exists():
        print(f"Error: File not found: {args.file1}")
        sys.exit(1)
    if not args.file2.exists():
        print(f"Error: File not found: {args.file2}")
        sys.exit(1)

    try:
        with open(args.file1, "r") as f1:
            data1 = json.load(f1)
        with open(args.file2, "r") as f2:
            data2 = json.load(f2)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        sys.exit(1)

    differences = fuzzy_compare(data1, data2)

    if not differences:
        print("JSON files match (fuzzy).")
        sys.exit(0)
    else:
        print(f"JSON files do NOT match. Found {len(differences)} difference(s):")
        if args.verbose:
            for d in differences:
                print(f" - {d}")
        else:
            # Print just the first few if not verbose
            for d in differences[:5]:
                print(f" - {d}")
            if len(differences) > 5:
                print(f" ... and {len(differences) - 5} more. Use --verbose to see all.")
        sys.exit(1)


if __name__ == "__main__":
    main()
