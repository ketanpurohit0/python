import heapq
from typing import List
from collections import defaultdict
from functools import reduce
from heapq import nlargest

def variance_calc_top_k(studentslist: List[str], scoreslist:List[int], k: int):

    # collect scores per student
    collection = defaultdict(list)
    for student, score in zip(studentslist, scoreslist):
        print(student, score)
        collection[student].append(score)

    print(collection)

    at_least_k = {
        student: heapq.nlargest(k, score)
        for student, score in collection.items() if len(score) >= k
    }

    print(at_least_k)

    result3 = {student: reduce(lambda v, s : v + (s-mean)**2, scores, 0) for student, scores, mean in [(student, scores, sum(scores) / len(scores)) for student, scores in at_least_k.items()]}
    print(result3)


if __name__ == '__main__':
    students = ["A", "A" , "A", "B", "B", "B", "C"]
    scores = [66, 23, 88, 90, 55, 66, 5]
    k = 3
    print(variance_calc_top_k(students, scores, k))
