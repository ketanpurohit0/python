# https://leetcode.com/problems/fancy-sequence/
from typing import Dict


class Fancy:
    ll: Dict[int, int] = {}
    idx: int = 0

    def __init__(self):
        self.ll = {}

    def add(l: int, m: int) -> int:
        return l+m

    def mult(l: int, m:int) -> int:
        return l*m

    def append(self, val: int) -> None:
        # self.ll(val)
        self.ll[self.idx] = val
        self.idx += 1

    def addAll(self, inc: int) -> None:
        # map(lambda x: x + inc, self.ll)
        for k in self.ll.keys():
            self.ll[k] = self.ll[k] + inc

    def multAll(self, m: int) -> None:
        for k in self.ll.keys():
            self.ll[k] = self.ll[k] * m

    def getIndex(self, idx: int) -> int:
        if 0 <= idx <= len(self.ll)-1:
            return self.ll[idx] % (10**9+7)
        else:
            return -1


def invoke_f(f, a, b):
    print(a, b)
    return f(a, b)


def foo(a, b, c):
    return a + b if c else a * b


if __name__ == '__main__':
    import functools
    import operator
    fadd = [Fancy.add, Fancy.add, Fancy.add]
    print(functools.reduce(Fancy.add, [1, 2, 3], 0))
    print(functools.reduce(operator.add, [1, 2, 3], 0))
    print(functools.reduce(operator.mul, [1, 2, 3], 1))
    print(invoke_f(Fancy.add, 3, 5))
    # print(functools.reduce(invoke_f,fadd,0))
    print(functools.reduce(functools.partial(foo, c=True), [1, 2, 3, 4, 5], 0))
    print(functools.reduce(functools.partial(foo, c=False), [1, 2, 3, 4, 5], 1))




if __name__ == '__foo__':
    fancy: Fancy = Fancy()
    fancy.append(2)  # fancy sequence: [2]
    fancy.addAll(3)  # fancy sequence: [2+3] -> [5]
    fancy.append(7)  # fancy sequence: [5, 7]
    fancy.multAll(2)  # fancy sequence: [5*2, 7*2] -> [10, 14]
    print(fancy.getIndex(0))  # return 10
    fancy.addAll(3)  # fancy sequence: [10+3, 14+3] -> [13, 17]
    fancy.append(10)  # fancy sequence: [13, 17, 10]
    fancy.multAll(2)  # fancy sequence: [13*2, 17*2, 10*2] -> [26, 34, 20]
    print(fancy.getIndex(0))  # return 26
    print(fancy.getIndex(1))  # return 34
    print(fancy.getIndex(2))  # return 20
