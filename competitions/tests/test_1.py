from src.competition import Solution420


def test_dmmy() -> None:
    left: int = 5
    right: int = 5
    assert left == right


def test_password_len() -> None:
    pwds = {"tosrt": (False, 1),
            "alllowercase": (True, 0),
            "ALLUPPER": (True, 0),
            "dfddsdsdffdfs": (True, 0),
            "ABBBBBBBBBBBBBBBBBBBBBBBBBBB": (False, 8)}
    for (pwd, expect) in pwds.items():
        # print(pwd, Solution420.minCharacters(pwd))
        assert(expect == Solution420.minCharacters(pwd))


def test_password_domain_check() -> None:

    pwds = {"tosrt": (False, 2),
            "alllowercase": (False, 2),
            "ALLUPPER": (False, 2),
            "dfddsdsdffdfs": (False, 2),
            "ABBBBBBBBBBBBBBBBBBBBBBBBBBB": (False, 2),
            "aA": (False, 1),
            "aA0": (True, 0),
            "zZ9": (True, 0)}
    for (pwd, expect) in pwds.items():
        assert(expect == Solution420.domainCheck(pwd))


def test_password_repeats() -> None:

    pwds = {
            "AAAtosrtZZZ": (False, 2),
            "ALLUPPER": (True, 0),
            "dfddsdsdffdfs": (True, 0),
            "aA": (True, 0),
            "aA0": (True, 0),
            "zZ9": (True, 0),
            "ThisIIISnotaG00dPass": (False, 1),
            "AAABBBCCC": (False, 3)}
    for (pwd, expect) in pwds.items():
        assert(expect == Solution420.repeatCheck(pwd))


def testPasswords() -> None:

    pwds = {"this1sAGoodPass": (True, 0),
            "thisisnotagoodpass": (False, 2),
            "THISISNOTAGOODPASS": (False, 2),
            "ThisIIISnotaG00dPass": (False, 1)}

    for (pwd, expect) in pwds.items():
        rlist = [Solution420.minCharacters(pwd), Solution420.domainCheck(pwd), Solution420.repeatCheck(pwd)]
        flags = [x for (x, _) in rlist]
        errs = sum(y for (_, y) in rlist)
        assert(expect == (all(flags), errs))


def testSolution() -> None:
    pwds = {"this1sAGoodPass": 0,
            "thisisnotagoodpass": 2,
            "THISISNOTAGOODPASS": 2,
            "thisi": 3,
            "THISI": 3,
            "TTT": 6,
            "AAAAAAAAAAAAAAAAAAAAAAAAA": 15,
            "ThisIIISnotaG00dPass": 1}

    f = Solution420()

    for (pwd, expect) in pwds.items():
        nErrors = f.strongPasswordChecker(pwd)
        assert(expect == nErrors)
        # list = [Solution420.minCharacters(pwd), Solution420.domainCheck(pwd), Solution420.repeatCheck(pwd)]
        # assert(expect == list.count(False))
