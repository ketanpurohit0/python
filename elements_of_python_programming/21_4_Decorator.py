import functools


def null_decorator(f):
    print("D")
    return f


@null_decorator
def foo():
    print("F")


def wrapped_decorator(f):
    def wrapper(*args, **kwargs):
        print("WD")
        r = f(*args, **kwargs)
        return r

    return wrapper


@wrapped_decorator
def foo2():
    print("F2")


def debuggable_wrapped_decorator(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        print("DWD")
        r = f(*args, **kwargs)
        return r

    return wrapper


@debuggable_wrapped_decorator
def foo3():
    print("F3")


if __name__ == '__main__':
    foo()
    null_decorator(foo)()
    print(null_decorator(foo).__name__)

    print("*")

    foo2()
    wrapped_decorator(foo2)()
    print(wrapped_decorator(foo2).__name__)

    print("*")

    foo3()
    debuggable_wrapped_decorator(foo3)()
    print(debuggable_wrapped_decorator(foo3).__name__)
