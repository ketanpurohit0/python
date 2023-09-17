import shutil
import tempfile
import unittest
from collections import defaultdict

from hypothesis import note, settings
from hypothesis.database import DirectoryBasedExampleDatabase
from hypothesis.stateful import RuleBasedStateMachine, rule, invariant, Bundle
from hypothesis.strategies import integers
import hypothesis.strategies as st


class DieHardProblem(RuleBasedStateMachine):
    small = 0
    big = 0

    @rule()
    def fill_small(self):
        self.small = 3

    @rule()
    def fill_big(self):
        self.big = 5

    @rule()
    def empty_small(self):
        self.small = 0

    @rule()
    def empty_big(self):
        self.big = 0

    @rule()
    def pour_small_into_big(self):
        old_big = self.big
        self.big = min(5, self.big + self.small)
        self.small = self.small - (self.big - old_big)

    @rule()
    def pour_big_into_small(self):
        old_small = self.small
        self.small = min(3, self.small + self.big)
        self.big = self.big - (self.small - old_small)

    @invariant()
    def physics_of_jugs(self):
        assert 0 <= self.small <= 3
        assert 0 <= self.big <= 5

    @invariant()
    def die_hard_problem_not_solved(self):
        note("> small: {s} big: {b}".format(s=self.small, b=self.big))
        assert self.big != 4


# The default of 200 is sometimes not enough for Hypothesis to find
# a falsifying example.
# with settings(max_examples=2000):
#     DieHardTest = DieHardProblem.TestCase
def heapnew():
    return []


def heapempty(heap):
    return not heap


def heappush(heap, value):
    heap.append(value)
    index = len(heap) - 1
    while index > 0:
        parent = (index - 1) // 2
        if heap[parent] > heap[index]:
            heap[parent], heap[index] = heap[index], heap[parent]
            index = parent
        else:
            break


def heappop(heap):
    return heap.pop(0)


def heapmerge(x, y):
    x, y = sorted((x, y))
    return x + y

class HeapMachine(RuleBasedStateMachine):
    Heaps = Bundle('heaps')

    @rule(target=Heaps)
    def newheap(self):
        return []

    @rule(heap=Heaps, value=integers())
    def push(self, heap, value):
        heappush(heap, value)

    @rule(heap=Heaps.filter(bool))
    def pop(self, heap):
        correct = min(heap)
        result = heappop(heap)
        assert correct == result

    @rule(target=Heaps, heap1=Heaps, heap2=Heaps)
    def merge(self, heap1, heap2):
        return heapmerge(heap1, heap2)


class DatabaseComparison(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.tempd = tempfile.mkdtemp()
        self.database = DirectoryBasedExampleDatabase(self.tempd)
        self.model = defaultdict(set)

    keys = Bundle("keys")
    values = Bundle("values")

    @rule(target=keys, k=st.binary())
    def add_key(self, k):
        return k

    @rule(target=values, v=st.binary())
    def add_value(self, v):
        return v

    @rule(k=keys, v=values)
    def save(self, k, v):
        self.model[k].add(v)
        self.database.save(k, v)

    @rule(k=keys, v=values)
    def delete(self, k, v):
        self.model[k].discard(v)
        self.database.delete(k, v)

    @rule(k=keys)
    def values_agree(self, k):
        assert set(self.database.fetch(k)) == self.model[k]

    def teardown(self):
        shutil.rmtree(self.tempd)



TestTrees = HeapMachine.TestCase

# Or just run with pytest's unittest support
if __name__ == "__main__":
    unittest.main()
