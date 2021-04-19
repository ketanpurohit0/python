import pytest


def test_bytes():
    b = bytes((1, 2, 3))
    assert(len(b) == 3)
    assert(b[0] == 1)
    with pytest.raises(TypeError):
        b[0] = 1
        del b[0]

    with pytest.raises(ValueError):
        i = bytes((1, 300))


def test_bytearray():
    b = bytearray((1, 2, 3))
    assert(len(b) == 3)
    assert(b[0] == 1)
    b[0] = 1
    del b[0]

    with pytest.raises(ValueError):
        i = bytearray((1, 300))
