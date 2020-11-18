# https://www.hackerrank.com/challenges/the-power-sum/problem

import sys
import math


# Complete the powerSum function below.
def powerSum(X, N):
    largestNPower = math.floor(math.pow(X, float(1/N)))
    seq = [x for x in range(largestNPower, 0, -1)]
    return powerSumInner(X, N, seq)


def isWholeRootOfN(X, N):
    # return X - math.pow(math.floor(math.pow(X, float(1/N))), N)
    if X > 0:
        return X == math.pow(math.floor(0.0000001 + math.exp(math.log(X)/N)), N)
    else:
        return True


def powerSumInner(X, N, seq):
    r = []
    for s in seq:
        for x in range(s, 0, -1):
            # print(internal_s)
            powerval = pow(x, N)
            remainderval = X - powerval
            if (remainderval in [0, 1]):
                r.append((x, powerval, remainderval))
            else:
                r.append(powerSum(remainderval, N))
    return r


if __name__ == '__main__':


    X = 10
    N = 2
    result = powerSum(X, N)
    print("*")
    print(result)

    X = 100
    N = 2
    result = powerSum(X, N)
    print("*")
    print(result)

    X = 100
    N = 3
    result = powerSum(X, N)
    print("*")
    print(result)
