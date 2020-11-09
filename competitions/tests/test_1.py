from src.competition import Solution420


def test_dmmy() -> None:
    left: int = 5
    right: int = 5
    assert left == right


def test_password_len() -> None:
    pwds = {"tosrt": False,
            "alllowercase": True,
            "ALLUPPER": True,
            "dfddsdsdffdfs": True,
            "ABBBBBBBBBBBBBBBBBBBBBBBBBBB": False}
    for (pwd, expect) in pwds.items():
        # print(pwd, Solution420.minCharacters(pwd))
        assert(expect == Solution420.minCharacters(pwd))


def test_password_domain_check() -> None:

    pwds = {"tosrt": False,
            "alllowercase": False,
            "ALLUPPER": False,
            "dfddsdsdffdfs": False,
            "ABBBBBBBBBBBBBBBBBBBBBBBBBBB": False,
            "aA": False,
            "aA0": True,
            "zZ9": True}
    for (pwd, expect) in pwds.items():
        assert(expect == Solution420.domainCheck(pwd))


def test_password_repeats() -> None:

    pwds = {
            "tosrt": True,
            "ALLUPPER": True,
            "dfddsdsdffdfs": True,
            "aA": True,
            "aA0": True,
            "zZ9": True}
    for (pwd, expect) in pwds.items():
        assert(expect == Solution420.repeatCheck(pwd))


def testPasswords() -> None:

    pwds = {"this1sAGoodPass": True,
            "thisisnotagoodpass": False,
            "THISISNOTAGOODPASS": False,
            "ThisIIISnotaG00dPass" : False}

    for (pwd, expect) in pwds.items():
        list = [Solution420.minCharacters(pwd), Solution420.domainCheck(pwd), Solution420.repeatCheck(pwd)]
        assert(expect == all(list))
