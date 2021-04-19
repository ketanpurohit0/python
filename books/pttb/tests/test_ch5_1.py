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
    assert(cm['one'] == 1)
    cm['3'] = '3'
    assert(dict1['3'] == '3')


def test_ro_dict():
    from types import MappingProxyType
    rw = {1: 1, 2: 2}
    ro = MappingProxyType(rw)
    assert(ro[1] == 1)
    with pytest.raises(TypeError):
        ro[2] = 2
