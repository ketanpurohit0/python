from collections import  deque


def solution(i):
    if len(i) % 2 == 0:
        return -1
    elif len(i) == 1:
        return 0

    dq = deque(i)

    all_match = True
    possible_answer = (len(i)-1)//2
    for n in range(0, possible_answer):
        all_match = dq.popleft() == dq.pop()
        if not all_match:
            break

    if all_match:
        return possible_answer
    else:
        return -1


if __name__ == '__main__':
    inputs = ["x","racecar","madam","aaabbb","xxaxx","cifXfic"]

    for i in inputs:
        print(i,"->",solution(i))