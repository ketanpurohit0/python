import inspect

if __name__ == "__main__":

    # due to i being a shared variable, the following evaluates
    increment_by_i = [lambda x: x + i for i in range(10)]
    print(increment_by_i[3](4))  # 13 -> 3 + 10
    print(increment_by_i[3](5))  # 14 -> 4 + 10
    src = inspect.getsource(increment_by_i[3])
    print(src)

    # now i is no longer shared
    def inc_by_i(i):
        return lambda x: x + i

    increment_by_i = [inc_by_i(i) for i in range(10)]
    print(increment_by_i[3](4))
    print(increment_by_i[3](5))
    src = inspect.getsource(increment_by_i[3])
    print(src)
