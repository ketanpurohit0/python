import heapq


class T:
    def __init__(self,a,b):
        self.a = a
        self.b = b

    def __lt__(self, other):
        if self.a < other.a:
            result = True
        elif (self.a == other.a) and self.b < other.b:
            result = True
        else:
            result = False

        return result

    def __repr__(self) -> str:
        return f"({self.a},{self.b})"

def solution(a, k):

    #a = list(map(lambda x: x* - 1, a))
    heapq.heapify(a)
    for i in range(len(a)):
        i = heapq.heappop(a)
        print(i)

    #return -1*heapq.heappop(a)


if __name__ == '__main__':
    inputs = [
    # [(13,"k","l"),(5,"u"), (11,"l")],
      [T(1,3),T(3,1),T(5,1),T(2,2),T(1,4),T(1,2)]
    ]

    for i in inputs:
        print(i, "->", solution(i,1))
        print(i, "->", solution(i,2))
        print(i, "->", solution(i,5))