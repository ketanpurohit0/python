def solution(n):

    def even_iterator(input):
        num = input
        while num % 2 == 0:
            yield num
            num = num // 2

    return len(list(even_iterator(n)))


if __name__ == '__main__':

    inputs = [24, 32, 11, 256, 99, 1000000000]
    for i in inputs:
        print(solution(i))