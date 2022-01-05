import random


def solution(S):
    from collections import deque

    def inner(input):
        dq = deque()
        prev = None
        for s in input:
            if prev != s:
                dq.append(s)
                prev = s
            else:
                dq.pop()
                prev = None

        result = "".join(list(dq))
        if not (result == "" or result == input):
            result = inner(result)

        return result

    return inner(S)


if __name__ == '__main__':
    inputs = [
        "ACCAABBC",
        "ABCBBCBA",
        "BABABA",
        "BBB",
        "BBBB",
        "".join(random.choice("AABBCC") for k in range(13)),
        "".join(random.choice("ABC") for k in range(10)),
        "".join(random.choice("AAABBBCCC") for k in range(20))
    ]

    inputs = [ "".join(random.choice("AAABBBCCC") for k in range(20)) for n in range(25)]
    for i in inputs:
        print(f'"{i}"')# "->", solution(i))
