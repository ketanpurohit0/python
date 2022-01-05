def solution(A):
    # for (jx, vj) in enumerate(A):
    #      for (ix, vi) in [(ix, vi) for ix, vi in enumerate(A) if ix < jx]:
    #          # print((ix, jx), (vi, vj), vj < vi)
    #          if (vj < vi):
    #              pass
    #              # print(ix, jx)
    result = len([(ix, jx) for (jx, vj) in enumerate(A) for (ix, vi) in [(ix, vi) for ix, vi in enumerate(A) if ix < jx] if
              vj < vi])
    if result > 1_000_000_000:
        return -1
    else:
        return result

def solution2(A):
    inversions = 0
    for idx in range(1, len(A)):
        #print(idx, A[idx], A[:idx])
        #print([v for v in A[:idx] if A[idx] < v])
        inversions += len([v for v in A[:idx] if A[idx] < v])
        if inversions > 1_000_000_000:
            inversions = -1
            break

    return inversions

def solution3(A):
    sortedA = sorted(A)
    print(A)
    print(sortedA)
    print([i for i in range(len(A)) if sortedA[i] != A[i]])



if __name__ == '__main__':
    inputs = [
        [-1, 6, 3, 4, 7, 4],
        [1, 2, 3, 4, 5],
        [5, 4, 3, 2, 1]
    ]

    for i in inputs:
        print(i, "->", solution3(i))
