import pytest


def test_bytes():
    a = b'h\x65llo'
    print(a)
    print(str(a))
    print(repr(a))
    print(list(a))
    assert (isinstance(a, bytes))
    assert (not isinstance(a, str))
    assert (isinstance(a.decode(), str))


def test_str():
    a = 'a\u0300 propos'
    print(a)
    print(list(a))
    assert (not isinstance(a, bytes))
    assert (isinstance(a, str))
    assert (isinstance(a.encode(), bytes))


def test_str_bytes_add():
    b1 = b'abc'
    b2 = b'def'
    b1 + b2

    s1 = str('abc')
    s2 = str('def')
    s1 + s2

    with pytest.raises(TypeError):
        s1 + b1

    with pytest.raises(TypeError):
        b2 + s2


def test_str_bytes_compare():
    b1 = b'abc'
    s1 = str('abc')
    with pytest.raises(TypeError):
        b1 > s1

    with pytest.raises(TypeError):
        s1 > b1

    assert(s1 != b1)


def test_write_bin():
    b1 = b'abc\xf1\xf2'
    with open("./temp.bin", "w") as f:
        with pytest.raises(TypeError):
            f.write(b1)

    with open("./temp.bin", "wb") as f:
        f.write(b1)

    with open("./temp.bin", "rb") as f:
        data = f.read()
        assert(data == b1)

    with open("./temp.bin", "r") as f:
        data = f.read()
        assert(data != b1)

