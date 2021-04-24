import pytest


def test_set():
    s = set()
    assert (len(s) == 0)

    s.add("foo")
    s.add("foo")
    assert (len(s) == 1)


def test_set2():
    s = set({"alice"})
    assert (len(s) == 1)
    for i in s:
        print(i)

    s = set("alice")
    assert (len(s) == 5)

    for i in s:
        print(i)

    s.add("Foo")


def test_frozenset():
    s = frozenset({"alice"})
    assert (len(s) == 1)
    for i in s:
        print(i)

    s = frozenset("alice")
    assert (len(s) == 5)

    for i in s:
        print(i)

    with pytest.raises(AttributeError):
        s.add("foo")

    d = {s: "Foo"}
    assert (d[s] == "Foo")


def test_multiset():
    from collections import Counter
    c = Counter({"sword": 1, "bread": 3})
    c.update({"bread": 1, "apple": 2})
    assert (c.get("bread") == 4)


def test_list_as_stack():
    l = []
    l.append("A")
    l.append("B")
    l.append("C")

    assert (l.pop() == "C")
    assert (l.pop() == "B")
    assert (l.pop() == "A")

    with pytest.raises(IndexError):
        l.pop()


def test_deque():
    from collections import deque
    dq = deque()
    dq.append("A")
    dq.append("B")
    dq.append("C")
    assert (dq.pop() == "C")
    assert (dq.pop() == "B")
    assert (dq.pop() == "A")

    dq.append("A")
    dq.append("B")
    dq.append("C")
    assert (dq.popleft() == "A")
    assert (dq.popleft() == "B")


def test_lifoqueue():
    from queue import LifoQueue, Empty
    lifo = LifoQueue()
    with pytest.raises(Empty):
        lifo.get(block=False, timeout=3)

    def producer(lifo: LifoQueue):
        lifo.put("Foo")

    def consumer(lifo: LifoQueue):
        assert (lifo.get() == "Foo")

    from threading import Thread

    t1 = Thread(target=producer, name="Producer", args=(lifo,))
    t1.start()
    print(lifo.qsize())

    t2 = Thread(target=consumer, name="Consumer", args=(lifo,))
    t2.start()
    print(lifo.qsize())


