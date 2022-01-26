import collections
import functools
import bint

def solution(A,B,C):
    from collections import  deque

    def inner(a, b):
        return a, b, b & a == b, a | b, b | a, a & (a | b), b & (a | b)

    dq = deque([A, B, C])
    dq2 = deque()
    dq3 = deque()
    for l in range(len(dq)):
        a = dq[0]
        b = dq[1]
        print ("<1>",inner(a, b))
        if not (b & a == b):
            dq2.append(a | b)
        dq.rotate()

    for l in range(len(dq2)):
        a = dq2[0]
        b = dq2[1]
        print("<2>",inner(a, b))
        if not (b & a == b):
            dq3.append(a | b)
        dq2.rotate()

    print(dq, dq2, dq3)
    # if B&A == B A is conforming to B
    #return A, B, B&A == B, A|B, B|A, A & (A|B), B & (A|B)

def hash(s: str, mod: int) -> int:
    mult = 997
    return functools.reduce(lambda z, c: (z * mult + ord(c)) % mod, s, 0)

def can_be_palindrome(s:str) -> bool:
    return sum(v % 2 for v in collections.Counter(s).values()) <= 1

if __name__ == '__main__':
    inputs = [

        (1073741727, 1073741631, 1073741679),
        (1,2,3)

    ]

    for i in inputs:
        #print(i, "->", solution(i[0], i[1], i[2]))
        print(hash(str(i[0]), 522))
        print(hash(str(i[1]), 522))
        print(hash(str(i[2]), 522))

    print(can_be_palindrome("abba"))
    print(can_be_palindrome("baba"))