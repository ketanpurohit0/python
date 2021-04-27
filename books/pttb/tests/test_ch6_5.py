import pytest


def generator(value: str):
    while True:
        yield value


def generator_with_state(value: str):
    local_state: int = 0
    while True:
        local_state += 1
        yield f"{value}.{local_state}"


def limited_generator(value: str, n: int):
    local_state: int = 0
    while local_state < n:
        local_state += 1
        yield f"{value}.{local_state}"
    # implicit end


def limited_generator_2(value: str, n: int):
    for i in range(n):
        yield f"{value}.{i}"


def test_generator():
    for v in generator("Hello"):
        assert (v == "Hello")


def test_generator_with_state():
    g = generator_with_state("Hello")
    r = next(g)
    assert (r == "Hello.1")
    r = next(g)
    assert (r == "Hello.2")


def test_limited_generator_with_state():
    n = 5
    var_generator = limited_generator("Hello", n)
    for e, g in enumerate(var_generator):
        print(g)

    assert (e == n - 1)

    with pytest.raises(StopIteration):
        next(var_generator)


def test_limited_generator2_with_state():
    n = 5
    var_generator = limited_generator_2("Hello", n)
    for e, g in enumerate(var_generator):
        print(g)

    assert (e == n - 1)

    with pytest.raises(StopIteration):
        next(var_generator)


def test_generator_expression():
    for i in (x for x in [1, 2, 3] if x > 1):
        assert (i == 2 or i == 3)


def test_nested_generator_expression():
    for tuples in ((e, o) for e in range(10) if e % 2 == 0 for o in range(10) if o % 2 == 1):
        print(tuples)
