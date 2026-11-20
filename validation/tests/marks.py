import functools

import pytest


def xfail(test, reason: str, **kwargs):
    """Mark an inherited test as an expected failure, keeping its own marks (e.g. parametrize)."""

    @functools.wraps(test)
    def wrapper(*args, **kw):
        return test(*args, **kw)

    return pytest.mark.xfail(reason=reason, **kwargs)(wrapper)
