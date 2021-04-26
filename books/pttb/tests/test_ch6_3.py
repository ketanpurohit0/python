def test_list1():
    lst = [1, 2, 3, 4, 5]
    assert (lst[1:3:1] == [2, 3])


def test_list2():
    lst = [1, 2, 3, 4, 5, 6]
    assert (lst[::2] == [1, 3, 5])


def test_list_r():
    lst = [1, 2, 3]
    assert (lst[::-1] == [3, 2, 1])


def test_del():
    lst = [1, 2, 3]
    del lst[:]
    assert(lst == [])


def test_ref():
    lst = [1, 2, 3]
    ref = lst
    assert(lst is ref)
    lst[:] = [4, 5, 6]
    assert(lst == [4, 5, 6])
    assert(lst is ref)