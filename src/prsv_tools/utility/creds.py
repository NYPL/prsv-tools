import configparser
from pathlib import Path

CREDS_INI = Path(__file__).parent.parent.parent.parent / "credentials.ini"


class PrsvCredentialException(Exception):
    pass


class Credentials(configparser.ConfigParser):
    def __init__(self, path: Path = CREDS_INI, *args, **kwargs):
        super().__init__(*args, **kwargs)

        path = Path(path)
        if not path.exists():
            raise PrsvCredentialException(
                f"Credentials file not found. Update the file at {str(CREDS_INI)}"
            )

        self.read(path)
        if not self.sections():
            raise PrsvCredentialException(
                f"Credentials file is empty. Update the file at {str(CREDS_INI)}"
            )

    def get_credential_sets(self) -> list[str]:
        return self.sections()

    def get_credentials(self, set: str) -> (str, str):
        if set not in self.sections():
            raise PrsvCredentialException(f"{set} is not a defined credential set")

        return (
            self.get_cred_value(set, "user"),
            self.get_cred_value(set, "pass"),
            self.get_cred_value(set, "tenant"),
        )

    def get_cred_value(self, set, key) -> str:
        if key not in self[set]:
            raise PrsvCredentialException(
                f"{set} is missing a field for {key}. Update the file at {str(CREDS_INI)}"
            )
        value = self[set][key]
        if not value:
            raise PrsvCredentialException(
                f"{set} is missing a value for {key}. Update the file at {str(CREDS_INI)}"
            )
        return self[set][key]
