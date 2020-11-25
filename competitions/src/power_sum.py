# https://www.hackerrank.com/challenges/the-power-sum/problem

import sys
import math


# Complete the powerSum function below.
def powerSum(X, N):
    largestNPower = math.floor(0.0000001 + math.exp(math.log(X)/N))
    seq = [powerSumInner(X, N, x) for x in range(largestNPower, 0, -1)]
    # return powerSumInner(X, N, seq)
    return seq


def isWholeRootOfN(X, N):
    # return X - math.pow(math.floor(math.pow(X, float(1/N))), N)
    if X > 0:
        return X == math.pow(math.floor(0.0000001 + math.exp(math.log(X)/N)), N)
    else:
        return True


def countWaysUtil(x, n, num):

    # Base cases
    val = (x - pow(num, n))
    if (val == 0):
        return 1
    if (val < 0):
        return 0

    # Consider two possibilities, num is
    # included and num is not included.
    return countWaysUtil(val, n, num + 1) + countWaysUtil(x, n, num + 1)


# Returns number of ways to express
# x as sum of n-th power of two.
def countWays(x, n):
    return countWaysUtil(x, n, 1)


def powerSumInner(X, N, x):
    powerval = pow(x, N)
    remainderval = X - powerval
    if (isWholeRootOfN(remainderval, N)):  
        print(X,N,x,remainderval)
        return 1
    else:
        largestNPower = math.floor(0.0000001 + math.exp(math.log(remainderval)/N))
        return powerSumInner(remainderval, N, largestNPower)



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
