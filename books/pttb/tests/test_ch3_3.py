# Decorators
def test_decorator_1():
    def null_decorator(callable_obj):
        return callable_obj

    def call_me(s: str):
        return s

    assert (call_me("A") == null_decorator(call_me)("A"))


def test_decorator_2():
    def decorate_upper(callable_obj):
        def wrapper():
            original = callable_obj()
            modified = original.upper()
            return modified

        return wrapper

    def decorate_exclaim(callable_obj):
        def wrapper():
            original = callable_obj()
            modified = original + '!'
            return modified

        return wrapper

    @decorate_upper
    def call_me():
        return 'a'

    @decorate_upper
    @decorate_exclaim
    def call_me_2():
        return 'a'

    assert (call_me() == 'A')
    assert (call_me_2() == 'A!')


def test_decorator_3():
    def decorate(callable_obj):
        def wrapper(*args, **kwargs):
            return callable_obj(*args, **kwargs)

        return wrapper

    @decorate
    def call_me(s: str):
        return s

    @decorate
    def call_me_2(s: str, n):
        return s + str(n)

    assert call_me('A') == 'A'
    assert call_me_2('A', 3) == 'A3'


def test_timer():
    def timeIt(callable_obj):
        import functools

        @functools.wraps(callable_obj)
        def wrapper(*args, **kwargs):
            import time
            start = time.time_ns()
            original = callable_obj(*args, **kwargs)
            return time.time_ns() - start, original

        return wrapper

    @timeIt
    def somework(n):
        from time import sleep
        sleep(n)

    assert somework(3)[0] >= 3 * 1_000_000_000
    assert (somework.__name__ == 'somework')
