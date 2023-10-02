import string

import pyparsing as pp


def grammar_one():
    """
    Start of Block
    :KeyWord::Mnemonic
    KeyWord = nnA
    Mnemonic = A...
    """

    keyword = pp.nums + string.ascii_uppercase
    mnemonic = string.ascii_uppercase
    grammar = ":" + pp.Word(keyword) + ":" + pp.Word(mnemonic)
    return grammar


def grammar_two():
    """
    Start of Block
    :KeyWord:Mnemonic
    KeyWord = nnA
    Mnemonic = A...
    """

    keyword = pp.Combine(
        pp.Word(pp.nums, min=2, max=2) + pp.Word(string.ascii_uppercase, min=1, max=1)
    )
    mnemonic = pp.Word(string.ascii_uppercase)
    grammar = ":" + keyword + ":" + mnemonic
    return grammar


def grammar_three():
    return grammar_two()[1,3]

def grammar_four():
    return pp.Group(grammar_two())[...]


if __name__ == "__main__":

    text = ":16R:SSIDET"
    print(text)
    for grammar in [grammar_one(), grammar_two(), grammar_three()]:
        print(grammar.parseString(text))

    for r in range(1,4):
        print(grammar_three().parseString(text*r))

    print(grammar_four().parseString(text*2))
