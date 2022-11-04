# prsv-tools
Modules and scripts to assist with moving files into Preservica at NYPL.
These modules attempt to implement the Preservica data model as generically as possible and may be used by other Preservica Users.
Where necessary, preference is given to NYPL use cases.

## Installation

```
pip install --user git+https://github.com/NYPL/prsv-tools.git
```

## Usage

If installed via `pip`, all scripts within `/bin` are installed as system-wide command-line tools and all modules are available for import.
All scripts include help documentation accessible via `-h`, e.g. `ingest_package.py -h`
