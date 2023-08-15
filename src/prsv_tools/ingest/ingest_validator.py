from pathlib import Path

import requests

import prsv_tools.utility.api as prsvapi

def ingest_has_correct_ER_number(collection_id) -> bool:

def main():
    """
    First type of check:
    1. Total number in one collection (M12234_ER_1234)
    2. Total number of file within objects and metadata

    Second type of check:
    1. If it is an SO & fits certain naming convention, do xxxxx
    2. If it is an IO, distinguish between content file and metadata file
       to check different checkpoints
    """


if __name__ == "__main__":
    main()