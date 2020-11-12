import src.new_year_chaos as nyc


def test_case_1() -> None:
    # tname -> size, [pattern], expected result
    testcases = {"t1": (5, [2, 1, 5, 3, 4], "3"),
                 "t2": (5, [2, 5, 1, 3, 4], "Too chaotic"),

                 "t3": (8, [5, 1, 2, 3, 7, 8, 6, 4], "Too chaotic"),
                 "t4": (8, [1, 2, 5, 3, 7, 8, 6, 4], "7"),

                 "t5": (8, [1, 2, 5, 3, 4, 7, 8, 6], "4")
                }

    for k, (ne, finalstate, expectedResult) in testcases.items():
        print(f"Test {k}")
        result = nyc.minimumBribes(finalstate)
        assert(result == expectedResult)