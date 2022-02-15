

def solution(K, A):

    max_mins = {}
    for i in range(0, len(A)):
        for j in range(i, len(A)):
            if i == j:
                max_mins[f"{i},{j}"] = (A[i],A[i])
            else:
                max_mins[f"{i},{j}"] = max(A[j], max_mins[f"{i},{j - 1}"][0]),  min(A[j], max_mins[f"{i},{j - 1}"][1])
            # print(i, j)
    # print(max_mins)
    return len(list(filter(lambda y: y[1][0]-y[1][1] <= K, max_mins.items())))
    # return result


if __name__ == '__main__':
    inputs = [
        [3, 5, 7, 6, 3],
        [3, 5, 7, 6, 3, 3, 5, 7, 6, 3],
        [3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3],
        [3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3],
        [3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3, 3, 5, 7, 6, 3]
    ]

    for i in inputs:
        print(i, "->", solution(2, i))
