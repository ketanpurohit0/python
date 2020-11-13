# https://leetcode.com/problems/fancy-sequence/
from typing import Dict, List


class Fancy:
    ops = []
    opp: List[int] = []
    valloc: List[int] = []
    idx: int = 0

    def __init__(self):
        self.ll = {}

    def add(ll: int, m: int) -> int:
        return ll+m

    def mult(ll: int, m: int) -> int:
        return ll*m

    def append(self, val: int) -> None:
        self.ops.append(self.append)
        self.opp.append(val)
        self.valloc.append(self.idx)
        self.idx += 1

    def addAll(self, inc: int) -> None:
        self.ops.append(self.add)
        self.opp.append(inc)
        self.idx += 1

    def multAll(self, m: int) -> None:
        self.ops.append(self.mult)
        self.opp.append(m)
        self.idx += 1

    def getIndex(self, idx: int) -> int:
        if (0 <= idx <= len(self.valloc)):
            pass
        else:
            return -1

    def info(self):
        fn = {self.append: "A",
              self.add: "+",
              self.mult: "*"}
        print("ops   :", [fn[f] for f in self.ops])
        print("opp   :", self.opp)
        print("valloc:", self.valloc)


def invoke_f(f, a, b):
    return f(a, b)


def foo(a, b, c):
    return a + b if c else a * b


if __name__ == '__foo__':
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


if __name__ == '__main__':
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
    fancy.info()
