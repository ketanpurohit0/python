# https://leetcode.com/problems/fancy-sequence/
from typing import Dict


class Fancy:
    ll: Dict[int, int] = {}
    idx: int = 0

    def __init__(self):
        self.ll = {}

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
