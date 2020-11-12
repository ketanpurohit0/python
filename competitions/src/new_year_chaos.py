from typing import List

# https://www.hackerrank.com/challenges/new-year-chaos/problem


def minimumBribes(q: List[int]):
    # decide if the list has impossible state
    result: str = "Too chaotic"
    if (validState(q)):
        swap: bool = True
        swapcnt: int = 0
        totalswaps: int = 0
        while(swap):
            (q, swap, swapcnt) = bsort(q)
            # print(q,swap,swapcnt)
            totalswaps += swapcnt
        result = str(totalswaps)
    print(result)
    return result


def bsort(q: List[int]):
    swap: bool = False
    swapcnt: int = 0
    for k in range(0, len(q)-1):
        # print(k, q)
        if (q[k] > q[k+1]):
            swap = True
            swapcnt += 1
            q[k+1], q[k] = q[k], q[k+1]
    return (q, swap, swapcnt)


def validState(q: List[int]) -> bool:
    # decide if the list has impossible state
    valid: bool = True
    sortedList = [x for x in range(1, len(q)+1)]
    for (l, r) in zip(q, sortedList):
        if (l-r) > 2:
            valid = False
            break
    return valid


if __name__ == '__main__':
    t = int(input())

    for t_itr in range(t):
        n = int(input())

        q = list(map(int, input().rstrip().split()))

        minimumBribes(q)
