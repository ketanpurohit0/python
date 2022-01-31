# Definition for singly-linked list.
from typing import Optional
from collections import deque


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


class Solution:
    def addTwoNumbers(self, l1: Optional[ListNode], l2: Optional[ListNode]) -> Optional[ListNode]:
        carry, rd = 0, 0
        loopcond = True
        while loopcond:
            (carry, rd) = ((l1.val + l2.val) // 10, (l1.val + l2.val) % 10)
            print(carry, "*", rd)
            loopcond = l1.next and l2.next
            if loopcond:
                l1 = l1.next
                l2 = l2.next

        n = l1.next if l1.next else l2.next

        while n.next:
            print(n.val)
            n = n.next


def walk(l: ListNode):
    if (l):
        print(l.val)
        walk(l.next)


def makeFromList(lseed):
    l = None
    p = None
    for i in range(len(lseed)):
        c = ListNode(lseed.popleft(), None)
        if p:
            p.next = c
        p = c
        if not l:
            l = c

    return l


if __name__ == '__main__':
    # 9,9,9,9,9,9,9
    lseed = deque([9, 9, 9, 9, 9, 9, 9])
    l = makeFromList(lseed)
    l2seed = deque([9, 9, 9, 9])
    l2 = makeFromList(l2seed)
    walk(l)
    walk(l2)

    s = Solution()
    s.addTwoNumbers(l, l2)
