import argparse
import re
import sys

def parse_args():
    parser = argparse.ArgumentParser(
        description='download files or packages from Preservica'
    )

    def is_valid_id(type: str, pattern: str, id: str) -> str:
        if not re.match(pattern, id):
            raise argparse.ArgumentTypeError(
                f'{id} does not match the expected {type} pattern, {pattern}'
            )
        return id

    def coll_id(id: str) -> str:
        return is_valid_id('collection ID', r'(M|L)\d\d\d+$', id)

    def object_id(id: str) -> str:
        return is_valid_id('SPEC object ID', r'(\d{6,7}$|all)', id)

    def er_id(id: str) -> str:
        return is_valid_id('electronic record number', r'((ER|DI|EM)_\d+$|all)', id)

    def ami_id(id: str) -> str:
        return is_valid_id('AMI ID', r'(\d{6}$|all)', id)


    ids = parser.add_argument_group(
        description='IDs that can be searched. At least 1 ID is required'
    )

    ids.add_argument(
        '--coll',
        required='--er' in sys.argv or ('--ami' in sys.argv and 'all' in sys.argv),
        type=coll_id,
        help='collection ID, M followed by 3-7 digits (M1234567)'
    )
    ids.add_argument(
        '--object',
        required=False,
        type=object_id,
        nargs='+',
        help='SPEC object ID, 6-7 digits (1234567), or "all"'
    )
    ids.add_argument(
        '--er',
        required=False,
        type=er_id,
        nargs='+',
        help='electronic record number, ER, DI, or EM followed by 1-4 digits (ER 123), requires a collection, or "all"'
    )
    ids.add_argument(
        '--ami',
        required=False,
        type=ami_id,
        nargs='+',
        help='AMI ID, 6 digits (123456), or "all"'
    )
    ids.add_argument(
        '--acq',
        required='--object' in sys.argv and 'all' in sys.argv,
        help='acquisition ID, unknown'
    )
    all_ids = [item.dest for item in ids._actions]
    if not any(f'--{id}' in sys.argv for id in all_ids):
       parser.error('at least one ID argument is required')

    parser.add_argument(
        '--search',
        action='store_true',
        default=True
    )

    return parser.parse_args()

def main():
    args = parse_args()

if __name__ == '__main__':
    main()