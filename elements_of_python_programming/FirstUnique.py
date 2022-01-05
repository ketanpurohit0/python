from collections import Counter


def solution(arr):
    counts = Counter(arr)
    unique_items = [k for k, v in counts.items() if v == 1]
    if len(unique_items):
        return unique_items[0]  # python preserves insertion order
    else:
        return -1

if __name__ == '__main__':
    inputs = [
        [4, 10, 5, 4, 2, 10],
        [1, 4, 3, 3, 1, 2],
        [6, 4, 4, 6]
    ]

    for i in inputs:
        print(i, "->", solution(i))