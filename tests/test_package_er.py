import sys


def test_run():
    import prsv_tools.ingest.package_er

    assert prsv_tools.ingest.package_er.config_input == "DA_config.ini"
    assert "prsv_tools.ingest.package_er" in sys.modules
