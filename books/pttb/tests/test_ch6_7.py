def test_iterator_chain():
    integers = range(8)
    squares = (s*s for s in integers)
    negated = (-1*n for n in squares)
    assert(list(negated) == [0, -1, -4, -9, -16, -25, -36, -49])


def test_iterator_chain2():
    integers = lambda n: range(n)
    squares = lambda it: (s*s for s in it)
    negated = lambda it: (-1*s for s in it)

    chain = [integers, squares, negated]
    param = 8
    for c in chain:
        param = c(param)

    if chain:
        # print(list(param))
        assert(list(param) == [0, -1, -4, -9, -16, -25, -36, -49])


def test_iterator_chain3():
    def integers(n): return range(n)
    def squares(it): return (s*s for s in it)
    def negated(it): return (-1*s for s in it)

    chain = [integers, squares, negated]
    param = 8
    for c in chain:
        param = c(param)

    if chain:
        # print(list(param))
        assert(list(param) == [0, -1, -4, -9, -16, -25, -36, -49])





