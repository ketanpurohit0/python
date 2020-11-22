# https://youtu.be/lyDLAutA88s

from collections import defaultdict


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
    # 18m
    data = list(csv.DictReader(open(r'data\data.csv')))
    assert(len(data) == 5)
    uniqueOnlycolC1 = {row["C1"] for row in data}
    allcolC1 = [row["C1"] for row in data]
    from collections import Counter
    c1 = Counter(uniqueOnlycolC1)
    c2 = Counter(allcolC1)
    print(c1)
    print(c2)
    # 24m
    upperC1 = [ {**row, 'C1' : row['C1'].upper()} for row in data]
    c3 = Counter([x['C1'] for x in upperC1])

def test_7():
    from collections import Counter
    c = defaultdict(Counter)
    c[1]["a"] += 1
    c[1]["b"] += 1
    c[2]["a"] += 1
    pass