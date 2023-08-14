import configparser

import pytest

import prsv_tools.utility.creds as prsvcreds

CRED_SETS = ["test-ingest", "prod-ingest", "test-manage", "prod-manage"]


@pytest.fixture
def good_credential_ini(tmp_path):
    config = configparser.ConfigParser()
    for cred_set in CRED_SETS:
        config[cred_set] = {"user": "user", "pass": "password", "tenant": cred_set[0:4]}

    config_file = tmp_path / "example.ini"
    with open(config_file, "w") as conf:
        config.write(conf)

    return config_file


def test_load_credentialfile(good_credential_ini):
    prsvcredentials = prsvcreds.Credentials(path=good_credential_ini)

    assert prsvcredentials


def test_error_on_missing_configfile(tmp_path):
    nonexistant = tmp_path / "nonexistant.ini"

    with pytest.raises(prsvcreds.PrsvCredentialException) as exc_info:
        prsvcreds.Credentials(path=nonexistant)

    assert (
        f"Credentials file not found. Update the file at {str(prsvcreds.CREDS_INI)}"
        in exc_info.value.args[0]
    )


def test_error_on_empty_configfile(tmp_path):
    emptyfile = tmp_path / "nonexistant.ini"
    emptyfile.touch()

    with pytest.raises(prsvcreds.PrsvCredentialException) as exc_info:
        prsvcreds.Credentials(path=emptyfile)

    assert "Credentials file is empty" in exc_info.value.args[0]


def test_require_sections(good_credential_ini):
    creds = prsvcreds.Credentials(path=good_credential_ini)

    assert CRED_SETS == creds.get_credential_sets()


@pytest.mark.parametrize("cred_set", CRED_SETS)
def test_accept_cred_request(good_credential_ini, cred_set: str):
    creds = prsvcreds.Credentials(path=good_credential_ini)
    user, pw, tenant = creds.get_credentials(set=cred_set)

    assert isinstance(user, str)
    assert isinstance(pw, str)
    assert isinstance(tenant, str)


def test_reject_nonexistant_cred_request(good_credential_ini):
    creds = prsvcreds.Credentials(path=good_credential_ini)
    section = "nonexistant"

    with pytest.raises(prsvcreds.PrsvCredentialException) as exc_info:
        creds.get_credentials(set=section)

    assert f"{section} is not a defined credential set" in exc_info.value.args[0]


@pytest.mark.parametrize("field", ["user", "pass", "tenant"])
def test_error_on_missing_field(good_credential_ini, field: str):
    creds = prsvcreds.Credentials(path=good_credential_ini)
    cred_set = CRED_SETS[0]
    creds.remove_option(cred_set, field)

    with pytest.raises(prsvcreds.PrsvCredentialException) as exc_info:
        creds.get_credentials(set=cred_set)

    assert f"{cred_set} is missing a field for {field}" in exc_info.value.args[0]


@pytest.mark.parametrize("field", ["user", "pass", "tenant"])
def test_error_on_bad_field_value(good_credential_ini, field: str):
    creds = prsvcreds.Credentials(path=good_credential_ini)
    cred_set = CRED_SETS[0]
    creds[cred_set][field] = ""

    with pytest.raises(prsvcreds.PrsvCredentialException) as exc_info:
        creds.get_credentials(set=cred_set)

    assert f"{cred_set} is missing a value for {field}" in exc_info.value.args[0]


"""
require instance

require username and pass
return error is user, pass DNE

require multiplerquire ini
"""
