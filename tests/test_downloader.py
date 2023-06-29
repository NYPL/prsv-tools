import bin.downloader as dldr

import pytest
from pathlib import Path

# Unit tests
## Argument tests

SINGLE_ID_ARGS = {
    'one_ami': {
        '--ami': '123456'
    },
    'one_spec_object': {
        '--object': '123456'
    }
}

SET_IDS = ['--coll', '--acq']

SET_ID_ARGS = {
    'one_er': {
        '--coll': 'M12345',
        '--er': 'ER_1'
    },
    'all_coll_ami': {
        '--coll': 'M12345',
        '--ami': 'all'
    },
    'all_coll_er': {
        '--er': 'all',
        '--coll': 'M12345'
    },
    'all_coll': {
        '--coll': 'M12345',
        '--er': 'all',
        '--ami': 'all'
    },
    'all_acq': {
        '--object': 'all',
        '--acq': '123456'
    }
}

ALL_ID_ARGS = {**SINGLE_ID_ARGS, **SET_ID_ARGS}


@pytest.mark.parametrize("scenario,ids", ALL_ID_ARGS.items())
def test_accept_valid_id_sets(monkeypatch, scenario: str, ids: dict):
    cmd = ['../bin/downloader.py']
    for id_type, id_value in ids.items():
        cmd.extend([id_type, id_value])

    monkeypatch.setattr('sys.argv', cmd)
    args = dldr.parse_args()

    for id_type, id_value in ids.items():
        assert id_value in getattr(args, id_type[2:])


@pytest.mark.parametrize("scenario,ids", ALL_ID_ARGS.items())
def test_reject_invalid_ids(monkeypatch, capsys, scenario: str, ids: dict):
    cmd = ['../bin/downloader.py']

    first_value = None
    for id_type, id_value in ids.items():
        if not first_value:
            first_value = id_value
        cmd.extend([id_type, f'@{id_value}@'])

    monkeypatch.setattr('sys.argv', cmd)
    with pytest.raises(SystemExit):
        dldr.parse_args()

    stderr = capsys.readouterr().err

    assert f'@{first_value}@ does not match the expected' in stderr


@pytest.mark.parametrize("scenario,ids", SET_ID_ARGS.items())
def test_reject_missing_ids(monkeypatch, capsys, scenario: str, ids: dict):
    cmd = ['../bin/downloader.py']

    for id_type, id_value in ids.items():
        if not id_type in SET_IDS:
            cmd.extend([id_type, id_value])
        else:
            required = id_type

    monkeypatch.setattr('sys.argv', cmd)
    with pytest.raises(SystemExit):
        dldr.parse_args()

    stderr = capsys.readouterr().err

    assert f'the following arguments are required: {required}' in stderr


@pytest.mark.parametrize("scenario,ids", ALL_ID_ARGS.items())
def test_accept_multiple_ids(monkeypatch, scenario: str, ids: dict):
    cmd = ['../bin/downloader.py']
    number_of_id_args = 6

    for id_type, id_value in ids.items():
        cmd.append(id_type)
        if not id_type in SET_IDS:
            cmd.extend([id_value] * number_of_id_args)
            final_type = id_type
            print(id_type)
        else:
            cmd.append(id_value)

    monkeypatch.setattr('sys.argv', cmd)
    args = dldr.parse_args()

    assert number_of_id_args == len(getattr(args, final_type[2:]))

def test_require_at_least_one_id(monkeypatch, capsys):
    cmd = ['../bin/downloader.py']

    monkeypatch.setattr('sys.argv', cmd)
    with pytest.raises(SystemExit):
        dldr.parse_args()

    stderr = capsys.readouterr().err

    assert f'at least one ID argument is required' in stderr