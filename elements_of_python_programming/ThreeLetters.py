import random
from collections import deque
from Tree import Tree

def solution(a,b):

    def popn(deq: deque):
        n = random.randint(1,2)
        if len(deq) > 0:
            if len(deq) >= n:
                return "".join([deq.pop() for i in range(0,n)])
            else:
                return "".join([deq.pop() for i in range(0,n-1)])
        else:
            return ""

    def solve(a, b):
        a_entries = deque("a"*a)
        b_entries = deque("b"*b)

        get = lambda n, m: (popn(a_entries), popn(b_entries)) if n > m else (popn(b_entries), popn(a_entries))

        print([get(a_entries, b_entries) for n in range(0, a+b)])
        #print(a_entries, b_entries)
        #print("a->",popn(a_entries))
        #print("b->",popn(b_entries))
        #print("a->",popn(a_entries))
        #print("b->",popn(b_entries))

    if a >= 3 or b >= 3:
        solve(a, b)

    else:
        return "a"*a + "b"*b

def solution2(a, b):

    def build(a_items, b_items, t, n):
        if n == 2:
            t.node_value

    a_entries = deque("a" * a)
    b_entries = deque("b" * b)

    root = Tree(a_entries.pop(), None, None) if (a < b) else Tree(b_entries.pop(), None, None)
    build(a_entries, b_entries, root, 1)



if __name__ == '__main__':

    inputs = [(1, 1), (2, 2), (5, 3), (3, 3), (1, 4)]
    for a,b in inputs:
        solution(a,b)