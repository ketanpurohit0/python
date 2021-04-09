from typing import Optional


def foo(*args, **kwargs):
    # print(required)
    # print(args)
    # print(kwargs)
    return args, kwargs


def bar(*args, **kwargs):
    args = args + ("BAR",)
    kwargs["FOO"] = 'BAR'
    # r1,r2 = foo(args, kwargs)
    return args, kwargs


def arg_unpack(x, y, z):
    return f"<{x},{y},{z}>"


class Car:
    def __init__(self, color: str, mileage: int):
        self.color = color
        self.mileage = mileage


class BlueCar(Car):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, *kwargs)
        self.color = 'blue'


def trace(callable_obj):
    import functools

    @functools.wraps(callable_obj)
    def wrapper(*args, **kwargs):
        if callable(callable_obj):
            print(callable_obj.__name__, args, kwargs)
            return callable_obj(*args, **kwargs)
        else:
            raise RuntimeError('A non-callable object was passed')

    return wrapper


def test_foo():
    r1, r2 = foo('1', '2', '3', k1="val1", k2="val2")
    assert (r1 == ('1', '2', '3'))
    assert (r2 == {'k1': 'val1', 'k2': 'val2'})

    r1, r2 = foo('1', k1="val1")
    assert (r1 == ('1',))
    assert (r2 == {'k1': 'val1'})

    r1, r2 = foo(k1='val1')
    assert (r1 == ())
    assert (r2 == {'k1': 'val1'})

    r1, r2 = foo('1')
    assert (r1 == ('1',))
    assert (r2 == {})

    r1, r2 = foo()
    assert (r1 == ())
    assert (r2 == {})


def test_bar():
    r1, r2 = bar('1', '2', '3', k1="val1", k2="val2")
    assert (r1 == ('1', '2', '3', 'BAR'))
    assert (r2 == {'k1': 'val1', 'k2': 'val2', 'FOO': 'BAR'})

    r1, r2 = bar('1', k1="val1")
    assert (r1 == ('1', 'BAR'))
    assert (r2 == {'k1': 'val1', 'FOO': 'BAR'})

    r1, r2 = bar(k1='val1')
    assert (r1 == ('BAR',))
    assert (r2 == {'k1': 'val1', 'FOO': 'BAR'})

    r1, r2 = bar('1')
    assert (r1 == ('1', 'BAR'))
    assert (r2 == {'FOO': 'BAR'})

    r1, r2 = bar()
    assert (r1 == ('BAR',))
    assert (r2 == {'FOO': 'BAR'})


def test_car():
    car = Car('Red', 1000)
    assert (car.color == 'Red')

    bluecar = BlueCar('Red', 1000)
    assert (bluecar.color == 'blue')


def test_trace():
    @trace
    def do_something(*args, **kwargs):
        pass

    do_something()
    do_something(k1='v1', k2='v2')
    do_something()
    do_something(k1='v1', k2='v2')


def test_argunpack():
    assert (arg_unpack(1, 2, 3) == "<1,2,3>")
    arr = [1, 2, 3]
    assert (arg_unpack(*arr) == "<1,2,3>")
    tup = (1, 2, 3)
    assert (arg_unpack(*tup) == "<1,2,3>")
    s = {1, 2, 3, 3}
    assert (arg_unpack(*s) == "<1,2,3>")
    dic = {"x": 1, "y": 2, "z": 3}
    assert (arg_unpack(**dic) == "<1,2,3>")
    dic2 = {"a": 1, "y": 2, "z": 3}
    try:
        assert (arg_unpack(**dic2) == "<1,2,3>")
    except TypeError:
        assert True
    else:
        assert False


def test_optional():
    def return_optional(a) -> Optional[str]:
        if a:
            return a
        return None

    assert (return_optional(None) is None)
    assert (return_optional(2) == 2)

    r = return_optional("2")
    assert (r == "2")
