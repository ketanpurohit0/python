# generate n random numbers
import random


# generator
def random_numbers(n):
    for i in range(n):
        yield random.random()


# iterator
class ExampleRandomIterator:
    def __init__(self, n: int):
        self.range = n

    def __iter__(self):
        return self

    def __next__(self):
        if self.range > 0:
            self.range -= 1
            return random.random()
        else:
            raise StopIteration()


if __name__ == '__main__':

    for idx, r in enumerate(random_numbers(10)):
        print(idx, r)

    random_iterator = ExampleRandomIterator(10)
    it = iter(random_iterator)
    for ra in range(21):
        print(next(it))
