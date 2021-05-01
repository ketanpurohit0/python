import pytest
from collections import defaultdict


def test_default_value_style_1():
    my_dict = dict({"a": 1, "b": 2})
    assert(my_dict.get("a") == 1)
    assert(my_dict.get("x") is None)
    with pytest.raises(KeyError):
        my_dict["x"]


def test_default_value_style_2():
    my_dict = defaultdict(int, a=1, b=2)
    assert(my_dict.get("a") == 1)
    assert(my_dict.get("x") is None)
    assert(my_dict["x"] == 0)


def test_sort_dict():
    my_dict = {"a": 1, "b": 2}
    assert(sorted(my_dict.items(), key=lambda x: x[1], reverse=True) == [('b', 2), ('a', 1)])
    assert(sorted(my_dict.items(), key=lambda x: x[0], reverse=True) == [('b', 2), ('a', 1)])


def test_sort_using_operator():
    import operator
    my_dict = {"a": 1, "b": 2}
    assert(sorted(my_dict.items(), key=operator.itemgetter(0), reverse=True) == [('b', 2), ('a', 1)])
    assert(sorted(my_dict.items(), key=operator.itemgetter(1), reverse=True) == [('b', 2), ('a', 1)])

