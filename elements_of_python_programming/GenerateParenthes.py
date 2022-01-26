from typing import List


def solution(i: int) -> str:
    def solve(n: int, diff : int, comb: List[str], combinations: List[str]):
        # print(n, diff, comb, combinations)
        if n == 0 and diff == 0:
            combinations.append("".join(comb))
            return
        elif diff < 0 or diff > n:
            return
        else:
            comb.append("(")
            solve(n - 1, diff + 1, comb, combinations)
            comb.pop()
            comb.append(")")
            solve(n - 1, diff - 1, comb, combinations)
            comb.pop()

    comb: List[str] = []
    combinations: List[str] = []
    solve(i, 0, comb, combinations)

    return combinations


if __name__ == '__main__':
    inputs = [4,6,7,66]

    for i in inputs:
        print(i, '->', solution(i))