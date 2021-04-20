import pytest


def test_ordered_dict():
    from collections import OrderedDict

    o = OrderedDict(a=1, b=2, c=3)
    l = list(o.keys())
    assert (l == ['a', 'b', 'c'])


def test_default_dict():
    from collections import defaultdict

    intDefaultDict = defaultdict(int)
    assert (intDefaultDict['somekey'] == 0)

    listDefaultDict = defaultdict(list)
    assert (listDefaultDict['fobar'] == [])

    listDefaultDict['mangydog'].append("foo")
    assert (listDefaultDict['mangydog'] == ['foo'])

    from collections import OrderedDict

    lx = defaultdict(OrderedDict)
    lx['zinc'] = 1
    lx['apple'] = 2
    assert (list(lx.keys()) == ['zinc', 'apple'])


def test_chainMap():
    from collections import ChainMap
    dict1 = {'1': '1', '2': '2'}
    dict2 = dict(one=1, two=2)
    cm = ChainMap(dict1, dict2)
    assert (cm['1'] == '1')
    assert (cm['one'] == 1)
    cm['3'] = '3'
    assert (dict1['3'] == '3')


def test_ro_dict():
    from types import MappingProxyType
    rw = {1: 1, 2: 2}
    ro = MappingProxyType(rw)
    assert (ro[1] == 1)
    with pytest.raises(TypeError):
        ro[2] = 2


def codility_leader(inputList):
    from collections import defaultdict
    lx = defaultdict(int)
    n = len(inputList)
    for item in inputList:
        lx[item] += 1

    items = [l for (l, r) in lx.items() if 2 * r > n]
    if len(items) == 0:
        return -1
    else:
        return items[0]


def random_list(bottom, top):
    import random
    n = random.randint(100000, 100000)
    randomValues = [random.randint(bottom, top) for _ in range(n)]
    return randomValues


@pytest.mark.parametrize("bottom, top", [(1, 2), (2, 5), (7, 7)])
def test_codility_leader(bottom, top):
    randomValues = random_list(bottom, top)
    lx = codility_leader(randomValues)
    print(lx)


def codility_equi(randomValues):
    total = sum(randomValues)
    # print(total)
    # print(randomValues)
    d = dict({-1: (0, total)})
    # print(d)
    for idx, valueAtIdx in enumerate(randomValues, 0):
        prev_left_sum, prev_right_sum = d[idx - 1]
        new_right_sum = prev_right_sum - valueAtIdx
        new_left_sum = total - (valueAtIdx + new_right_sum)
        d[idx] = (new_left_sum, new_right_sum)

    r = [i for i, (l, r) in d.items() if l == r]
    if len(r):
        return r[0], len(r)
    else:
        return -1, 0


def test_codility_equi():
    testv = [-1, 3, -4, 5, 1, -6, 2, 1]
    r = codility_equi(testv)
    assert (r == (1, 3) or r == (3, 3) or r == (7, 3))


@pytest.mark.parametrize("bottom, top", [(-3, 3), (-2, 2), (-7, 7)])
def test_codility_equi2(bottom, top):
    randomValues = random_list(bottom, top)
    randomValues2 = list(reversed(randomValues))
    randomValues.extend(randomValues2)
    r = codility_equi(randomValues)
    print(r)
