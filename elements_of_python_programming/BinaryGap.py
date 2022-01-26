def solution(N):
    s = bin(N)[2:]
    max_v = 0
    work_v = 0
    for ix, v in enumerate(s):
        if v == "1":
            max_v = max(work_v, max_v)
            work_v = 0
        else:
            work_v += 1

    return s, max_v


if __name__ == '__main__':
    inputs = [
        1, 3, 9, 529,  20, 15, 32, 1041
    ]

    for i in inputs:
        print(i, "->", solution(i))