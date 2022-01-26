def solution(K, A):
    from itertools import accumulate

    result = 0
    for i in range(0, len(A)):
        max_accumulator = accumulate(A[i:], max)
        min_accumulator = accumulate(A[i:], min)
        result += len([1 for m_ax, m_in in zip(max_accumulator, min_accumulator) if m_ax - m_in <= K])
        if result > 1_000_000_000:
            result = 1_000_000_000
            break
    return result

if __name__ == '__main__':
    inputs = [
        [3, 5, 7, 6, 3],
        [3, 5, 7, 6, 3, 3, 5, 7, 6, 3],
        [3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3],
        [3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3],
        [3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3]
    ]

    for i in inputs:
        print(i, "->", solution(2,i))