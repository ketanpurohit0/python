def test_priority_queue():
    from queue import PriorityQueue
    q = PriorityQueue(maxsize=10)
    q.put(item=(2, 'code'), timeout=5)
    q.put(item=(1, 'sleep'), timeout=1)
    q.put(item=(3, 'work'), timeout=3)

    assert(q.qsize() == 3)
    assert(q.get() == (1, 'sleep'))


def test_prioriry_queue_produce_consume():
    from queue import PriorityQueue
    from threading import Thread, current_thread

    def producer(q: PriorityQueue):
        import random
        for item in range(0, 3):
            p = random.randint(0, item)
            print(p, flush=True)
            q.put((p, item))

    def consumer(q: PriorityQueue):
        while not q.empty():
            item = q.get()
            print(f"[{item}]", current_thread().getName(),flush=True)

    pq = PriorityQueue()
    t1 = Thread(target=producer, args=(pq, ),name="PQ1")
    t2 = Thread(target=producer, args=(pq, ), name="PQ2")
    t1.start()
    t2.start()
    t3 = Thread(target=consumer, args=(pq, ),name="CQ1")
    t4 = Thread(target=consumer, args=(pq, ), name="CQ2")
    t5 = Thread(target=consumer, args=(pq, ),name="CQ3")
    t6 = Thread(target=consumer, args=(pq, ), name="CQ4")
    t3.start()
    t4.start()
    t5.start()
    t6.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()




