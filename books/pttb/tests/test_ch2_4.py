# Underscores, Dunders and more

def test_Test():
    import src.ch2_4 as ch2_4
    test = ch2_4.Test("val", "val")
    assert (test.first == test._second)


def test_internal_external():
    import src.ch2_4 as ch2_4
    ch2_4._internal_func()
    ch2_4.external_func()


def test_param():
    import src.ch2_4 as ch2_4
    assert "foo" == ch2_4.param_is_reserved_word("foo")


def test_name_mangling():
    import src.ch2_4 as ch2_4
    t = ch2_4.Test("foo", "bar")
    assert (t.first == "foo")
    assert (t._second == "bar")
    assert (t._Test__third == "foo")
    assert (t.get_third() == "foo")

    t2 = ch2_4.ExtendedTest("foo", "bar")
    assert (t2.first == "foo")
    assert (t2._second == "bar")
    assert (t2._ExtendedTest__third == "foo")
    assert (t2.get_third() == "foo")
    # Cannot see the following
    # assert (t2._Test__third == "foo")


