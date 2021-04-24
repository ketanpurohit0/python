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


def test_lifoQueue():
    from queue import LifoQueue, Empty
    from threading import Thread, current_thread

    lifo = LifoQueue()
    with pytest.raises(Empty):
        lifo.get(block=False, timeout=3)

    def producer(lifoIn: LifoQueue, n: int):
        for i in range(n):
            print("Producing", current_thread().getName())
            lifoIn.put("Foo")

    def consumer(lifoIn: LifoQueue):
        while not lifoIn.empty():
            print("Consuming", current_thread().getName())

            assert (lifoIn.get() == "Foo")

        with pytest.raises(Empty):
            assert(lifoIn.get_nowait())

    t1 = Thread(target=producer, name="Producer", args=(lifo,5))
    t1.start()
    assert(lifo.qsize()>= 0)

    t2 = Thread(target=consumer, name="Consumer", args=(lifo,))
    t2.start()
    assert(lifo.qsize() <= 5)

    t1.join()
    print("Producer joined")
    t2.join()
    print("Consumer joined")

    assert(lifo.qsize() == 0)


def test_fifoQueue():
    from queue import Queue, Empty
    from threading import Thread, current_thread
    fifo = Queue()
    with pytest.raises(Empty):
        fifo.get(block=False, timeout=5)

    def producer(pfifo: Queue, n: int):
        for i in range(n):
            print(f"Producing {i}  {current_thread().getName()}")
            pfifo.put(i)

    def consumer(pfifo: Queue):
        while not fifo.empty():
            try:
                i = fifo.get_nowait()
                print(f"Consuming {i}  {current_thread().getName()}")
            except Empty:
                pass

    producerThreads = [Thread(name=f"Produce{t}", args=(fifo, t, ), target=producer) for t in range(1, 4)]
    [thread.start() for thread in producerThreads]

    assert(0 <= fifo.qsize() <= 6)

    consumerThreads = [Thread(name=f"Consume{t}", target=consumer, args=(fifo, )) for t in range(4)]
    [thread.start() for thread in consumerThreads]

    [thread.join() for thread in producerThreads]
    [thread.join() for thread in consumerThreads]

    assert(fifo.qsize() == 0)

