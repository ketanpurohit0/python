# LeetCode/Hard/420. Strong Password Checker
class Solution420:
    regex: str = ""

    def strongPasswordChecker(self, s: str) -> int:
        return 0

    # It has at least 6 characters and at most 20 characters.
    def minCharacters(s: str) -> bool:
        return 6 <= len(s) <= 20

    # It must contain at least one lowercase letter, at least one uppercase letter, and at least one digit.
    def domainCheck(s: str) -> bool:
        regex: str = ".*[0-9].*"
        import re
        list = [(s.upper() != s), (s.lower() != s), (re.search(regex, s) != None)]
        return all(list)

    # It must NOT contain three repeating characters in a row
    def repeatCheck(s: str) -> bool:
        regex = r"(\w)\1\1"
        import re
        matches = re.search(regex, s)
        return (matches is None)
