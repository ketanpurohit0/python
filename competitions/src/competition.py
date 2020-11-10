# LeetCode/Hard/420. Strong Password Checker
class Solution420:
    regex: str = ""

    def strongPasswordChecker(self, s: str) -> int:
        list = [Solution420.minCharacters(s), Solution420.domainCheck(s), Solution420.repeatCheck(s)]
        fixes = max(x for (_, x) in list)
        return fixes

    # It has at least 6 characters and at most 20 characters.
    def minCharacters(s: str):
        corrections: int = 0
        if (len(s) < 6):
            corrections = 6-len(s)
        elif (len(s) > 20):
            corrections = len(s) - 20
        return (6 <= len(s) <= 20, corrections)

    # It must contain at least one lowercase letter, at least one uppercase letter, and at least one digit.
    def domainCheck(s: str):
        regex: str = ".*[0-9].*"
        import re
        list = [(s.upper() != s), (s.lower() != s), (re.search(regex, s) != None)]
        return (all(list), list.count(False))

    # It must NOT contain three repeating characters in a row
    def repeatCheck(s: str) -> bool:
        regex = r"(\w)\1\1"
        import re
        matches = re.findall(regex, s)
        return (len(matches) == 0, len(matches))
