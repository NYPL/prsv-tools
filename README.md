# prsv-tools
Modules and scripts to assist with moving files into Preservica at NYPL.
These modules attempt to implement the Preservica data model as generically as possible and may be used by other Preservica Users.
Where necessary, preference is given to NYPL use cases.

## Installation

```
pip install --user git+https://github.com/NYPL/prsv-tools.git
```

An example `.ini` is provided.
There must be at least one `.ini` file for every Preservica instance.

## Usage

Most modules within `src` are intended to be used as command-line scripts.
They can be called using their filename without the `.py`.
Check `[tool.poetry.scripts]` in the `pyproject.toml` file for installable scripts.

Modules within `utility` are intended to be used as imports to other scripts.
For example, import the Preservica token like this, `import prsv_tools.utility.prsvtoken as prsvtoken`.
