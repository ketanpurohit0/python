class Repeater:
    def __init__(self, value):
        self.value = value

    def __iter__(self):
        return RepeaterIterator(self)


class RepeaterIterator:
    def __init__(self, source):
        self.source = source

    def __next__(self):
        return self.source.value


class BetterRepeater:
    def __init__(self, value):
        self.value = value

    def __iter__(self):
        return self

    def __next__(self):
        return self.value

class BoundedRepeater:
    def __init__(self, value, max_repeats):
        self.value = value
        self.max_repeats = max_repeats

    def __iter__(self):
        return self

    def __next__(self):
        if self.max_repeats > 0:
            self.max_repeats -= 1
            return self.value
        else:
            raise StopIteration


def test_iterator_1():
    repeater = Repeater("Hello")
    for r in repeater:
        assert(r == "Hello")


def test_iterator_2():
    better_repeater = BetterRepeater("Hello")
    for r in better_repeater:
        assert(r == "Hello")


def test_iterator_3():
    bounded_iterator = BoundedRepeater("Hello", 5)
    for r in bounded_iterator:
        print(r)

    for r in iter(bounded_iterator):
        print(r)

    bounded_iterator = BoundedRepeater("Ketan", 7)
    assert(len([x for x in bounded_iterator]) == 7)
