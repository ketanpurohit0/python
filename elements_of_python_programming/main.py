from functools import cache, reduce, partial
stepSizes = list(range(1, 4))


@cache
def outcomes(steps: int):
    return [steps - stepSize for stepSize in stepSizes if steps - stepSize >= 0]


@cache
def outcomes2(steps: int):
    return [s for s in [steps-1, steps-2, steps-3] if s >= 0]


def solution(stepsInLadder: int) -> int:
    tree = {stepsInLadder: []}

    @cache
    def solution_inner(t: dict, s: int):
        steps_remaining = outcomes(s)
        if s in tree:
            t[s].extend(steps_remaining)
        else:
            t[s] = steps_remaining

        # print("*",t)

        steps_remaining = filter(lambda x: x > 0, steps_remaining)
        for s1 in steps_remaining:
            solution_inner(t, s1)
        # l = map(partial(solution_inner, t), steps_remaining)
        # pass

    solution_inner(tree, stepsInLadder)
    result = 0
    for v in tree.values():
        result += len(list(filter(lambda x: x == 0, v)))
    return result


def solution2(stepsInLadder: int) -> int:
    @cache
    def solution_inner(s: int):
        possible_remaining_steps = outcomes2(s)
        n_zero_outcomes = sum(1 for _ in filter(lambda x: x == 0, possible_remaining_steps))
        n_nonzero_outcomes = filter(lambda x: x > 0, possible_remaining_steps)
        for s1 in n_nonzero_outcomes:
            n_zero_outcomes += solution_inner(s1)

        return n_zero_outcomes

    return solution_inner(stepsInLadder)


def testf(i: int, j: int):
    return i+j


if __name__ == "__main__":
    # answer = [solution(r) for r in range(1, 25)]
    # print(answer)
    answer = [solution2(r) for r in range(1, 150)]
    print(answer)

    m = map(partial(testf, 5), answer)
    print(list(m))

    answer = map(solution2, range(1,150))
    print(list(answer))

    print(solution2(1300))

