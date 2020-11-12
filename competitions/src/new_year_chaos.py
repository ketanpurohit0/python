from typing import List


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
    q = [1, 2, 5, 3, 7, 8, 6, 4]
    q = [2, 1, 5, 3, 4]
    q = [5, 1, 2, 3, 7, 8, 6, 4]
    q = [1, 2, 5, 3, 4, 7, 8, 6]
    print(minimumBribes(q))
    """
    swap: bool = True
    swapcnt: int = 0
    totalswaps: int = 0
    while(swap):
        (q, swap, swapcnt) = bsort(q)
        # print(q,swap,swapcnt)
        totalswaps += swapcnt
    print(totalswaps)
    """
