import bin.lint_er as lint_er


# Unit tests
## Argument tests
def test_accepts_paths(monkeypatch):
    """Test that packages are returned as a list"""
    paths = ['firstPath', 'secondPath']

    monkeypatch.setattr(
        'sys.argv', [
            '../bin/lint_er.py',
            '--package', paths[0], paths[1]
        ]
    )

    args = lint_er.parse_args()
    assert paths == args.packages


# Functional tests
def test_lint_valid_package():
    """Run entire script with valid ER"""
    lint_er.main()
    assert False

def test_lint_invalid_package():
    """Run entire script with valid ER"""
    lint_er.main()
    assert False

