import pytest


def test_deprecation_warning():
    with pytest.warns(DeprecationWarning):
        import hdmf.monitor  # noqa: F401
