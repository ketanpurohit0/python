import pytest


def test_1():
    assert True, "failed"


@pytest.mark.parametrize("p1,p2",
                         [(1, 1), (2, 2), (3, 3)]
                         )
def test_param(p1, p2):
    assert (p1 == p2)
