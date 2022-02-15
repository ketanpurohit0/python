from typing import List


def solution(arr: List[int]) -> List[int]:
    def swapElements(i: int, j: int) -> None:
        print(i,j)
        arr[i], arr[j] = arr[j], arr[i]

    def checkGreater(i, j) -> bool:
        return arr[i] > arr[j]

    def checkLess(i, j) -> bool:
        return arr[i] < arr[j]

    fnMap = {0: checkLess, 1: checkGreater}

    for i in range(len(arr)-1):
        j = i + 1
        fn = fnMap[i % 2]
        if not (fn(i, j)):
            swapElements(i, j)

    return arr


if __name__ == '__main__':

    inputs = [
        [1, 2, 3, 4, 5, 6],
        [7, 4, 5, 1, 3, 2],
        [3, 5, 2, 1, 6, 4],
        [1, 2, 3, 4]
    ]

    for i in inputs:
        prev = i.copy()
        print(prev, '->', solution(i))
