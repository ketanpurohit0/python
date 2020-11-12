# https://leetcode.com/problems/fancy-sequence/
from typing import List


class Fancy:
    ll: List[int] = []

    def __init__(self):
        self.ll = []

    def append(self, val: int) -> None:
        self.ll.append(val)

    def addAll(self, inc: int) -> None:
        self.ll = list(map(lambda x: x + inc, self.ll))

    def multAll(self, m: int) -> None:
        self.ll = list(map(lambda x: x * m, self.ll))

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
