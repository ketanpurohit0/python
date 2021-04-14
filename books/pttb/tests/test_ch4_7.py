class Foo:
    stat = 7

    def __init__(self, name):
        self.name = name


class Counted:
    __count = 0

    def __init__(self):
        Counted.__count += 1

    @staticmethod
    def count():
        return Counted.__count


def test_foo():
    f = Foo("Ketan")
    assert (f.name == "Ketan")
    assert (f.stat == 7)


def test_foo2():
    f1 = Foo("bar")
    f2 = Foo("foo")
    Foo.stat = 11
    assert(f1.stat == 11)
    assert(f2.stat == 11)


def test_counted():
    assert(Counted.count() == 0)
    Counted()
    assert(Counted.count() == 1)
    Counted()
    assert(Counted.count() == 2)

    # Creates shadow variable
    Counted.__count = 5
    assert(Counted.__count == 5)
    assert(Counted.count() == 2)
