from typing import List


def delete_duplicates(A: List[int]) -> int:
    if not A:
        return 0

    write_index = 1
    for i in range(1, len(A)):
        if A[write_index - 1] != A[i]:
            A[write_index] = A[i]
            write_index+=1

    return write_index


if __name__ == '__main__':

    inputs = [
        [1,2,2,3,3],
        sorted([1,4,5,6,4,3,2,4,2,4,5])
    ]

    for i in inputs:
        print(i, "->", delete_duplicates(i))