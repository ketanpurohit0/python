from typing import List


def solution(arr: List[int]) -> None:
    span = []

    for index in range(len(arr)):
        if index == 0:
            span.append(1)
        else:
            if arr[index] < arr[index - 1]:
                span.append(1)
            else:
                thespan = 1 + span[index - 1]
                span.append(thespan)

    return span

if __name__ == '__main__':

    inputs = [
        [1, 2, 3, 4, 5],
        [100, 80, 60, 70, 60, 75, 85]
    ]

    for i in inputs:
        print(i, '->', solution(i))
