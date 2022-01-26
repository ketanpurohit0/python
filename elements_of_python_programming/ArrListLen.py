def solution(A):
    c = 0
    l = 0
    if len(A):
        while A[l] != -1:
            l = A[l]
            c += 1
    return c + 1 # for the -1


if __name__ == '__main__':
    inputs = [
        [1,4,-1,3,2],
        [2,3,-1,4,1]
    ]

    for i in inputs:
        print(i, "->", solution(i))