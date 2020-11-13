# https://leetcode.com/problems/fancy-sequence/
from typing import List


class Fancy:

    def __init__(self):
        self.fn = {self.append: "A",
                   self.add: "+",
                   self.mult: "*",
                  }

        self.ops = []
        self.valloc: List[int] = []
        self.idx: int = 0
        self.anyAppends: bool = False

    @staticmethod
    def add(ll: int, m: int) -> int:
        return ll+m

    @staticmethod
    def mult(ll: int, m: int) -> int:
        return ll*m

    def append(self, val: int) -> None:
        self.ops.append((self.append, val))
        self.valloc.append(self.idx)
        self.idx += 1
        self.anyAppends = True

    def addAll(self, inc: int) -> None:
        self.ops.append((self.add, inc))
        self.idx += 1

    def multAll(self, m: int) -> None:
        self.ops.append((self.mult, m))
        self.idx += 1

    def evaluate(self, idx: int) -> int:
        ridx = self.valloc[idx]
        # print(self.ops[ridx][1])
        sv = self.ops[ridx][1]
        # print(self.ops[ridx+1:])
        # print("ops   :", [(self.fn[f], v) for (f, v) in self.ops[ridx+1:] if f != self.append])
        x = [(f, v) for (f, v) in self.ops[ridx+1:] if f != self.append]
        for f, v in x:
            sv = f(sv, v)
        # print(sv)

        return sv % (10**9+7)

    def getIndex(self, idx: int) -> int:
        if (self.anyAppends) and (0 <= idx < len(self.valloc)):
            return self.evaluate(idx)
        else:
            return -1

    def info(self):
        print("ops   :", [(self.fn[f], v) for (f, v) in self.ops])
        print("valloc:", self.valloc)
        print(self.ops)


def invoke_f(f, a, b):
    return f(a, b)


def foo(a, b, c):
    return a + b if c else a * b


def gen():
    calls =   ["append","multAll","append","getIndex","addAll","append","append","getIndex","append","append","addAll","addAll","getIndex","append","getIndex","getIndex","addAll","getIndex","addAll","getIndex","getIndex","getIndex","append","addAll","append","getIndex","multAll","append","append","addAll","getIndex","addAll","getIndex"]
    params = [[2],[10],[8],[0],[9],[8],[10],[0],[10],[3],[6],[3],[3],[5],[4],[0],[8],[3],[6],[7],[4],[3],[1],[8],[5],[3],[3],[7],[5],[2],[3],[2],[9]]
    for (a, b) in zip(calls, params):
        print(f"fancy.{a}({b[0]})")


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
    # fancy.info()
    gen()

