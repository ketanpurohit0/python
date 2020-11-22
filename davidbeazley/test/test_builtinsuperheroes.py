# https://youtu.be/lyDLAutA88s

def test_1():
    x = ['a', 'b', 'c']
    y = [x.upper() for x in iter(x)]
    assert(y == ['A', 'B', 'C'])


def test_2():
    # frequency
    from collections import Counter
    names = ['A', 'B', 'A']
    s = set(names)
    c = Counter(names)
    assert(c['A'] == 2)
    c = Counter(s)
    assert(c['A'] == 1)


def test_3():
    # dict with list associated
    from collections import defaultdict
    s = defaultdict(list)
    s['a'].append('foo')
    s['a'].append('bar')
    assert(s['a'] == ['foo', 'bar'])


def test_4():
    # dict with set associated
    from collections import defaultdict
    s = defaultdict(set)
    s['a'].add('foo')
    s['a'].add('bar')
    assert(s['a'] == {'foo', 'bar'})


def test_5():
    nums = [1, 2, 3]
    squares = (x*x for x in nums)
    # squares is generator expression
    for x in squares:
        print(x)


def test_6():
    import csv
    data = list(csv.DictReader(open(r'data\data.csv')))
    assert(len(data) == 3)
    row = data[0]
