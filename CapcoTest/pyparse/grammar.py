import string

import pyparsing as pp


def grammar_one():
    """Start of Block
    :KeyWord::Mnemonic
    KeyWord = nnA
    Mnemonic = A...
    """
    keyword = pp.nums + string.ascii_uppercase
    mnemonic = string.ascii_uppercase
    return ":" + pp.Word(keyword) + ":" + pp.Word(mnemonic)


def grammar_two():
    """Start of Block
    :KeyWord:Mnemonic
    KeyWord = nnA
    Mnemonic = A...
    """
    keyword = pp.Combine(
        pp.Word(pp.nums, min=2, max=2) + pp.Word(string.ascii_uppercase, min=1, max=1),
    )
    mnemonic = pp.Word(string.ascii_uppercase)
    return ":" + keyword + ":" + mnemonic


def grammar_three():
    return pp.Group(grammar_two())[1,3]

def grammar_four():
    return pp.Group(grammar_two())[...]

def grammar_five():
    """Sender's Message Reference."""
    """:20C::SEME//345678"""

    return pp.Literal(":20C::SEME") + "//" + pp.Word(pp.alphanums)

def grammar_six():
    """Sender's Message Reference."""
    """:20C::SEME//345678"""

    return pp.Literal(":20C::SEME//").suppress() + pp.Word(pp.alphanums).setResultsName("ID")

def grammar_seven():
    """Sender's Message Reference."""
    """:20C:A"""

    return pp.Literal(":20C:").suppress() + pp.oneOf("A B")

def grammar_50h():
    """sample=
    :50H:/344110001637
    TESTAR00AXXX
    Utrecht
    Netherlands
    format=
    Option H	/34x     (Account)
                4*35x    (Name and Address).

    """
    return (pp.Literal(":50H:") +
            pp.Literal("/") + pp.Word(pp.alphanums + string.punctuation + " ", min=1, max=34).setResultsName("Account") + pp.lineEnd +
        pp.Group(pp.Word(pp.alphanums + string.punctuation + " "))[1, 4].setResultsName("NameAndAddress")).setResultsName("OrderingCustomer")

def grammar_bic():
    return pp.Combine(pp.Word(pp.alphanums, min=11, max=11) ^ pp.Word(pp.alphanums, min=8, max=8))


if __name__ == "__main__":

    text = ":16R:SSIDET"
    print(text)
    for grammar in [grammar_one(), grammar_two(), grammar_three()]:
        print(grammar.parseString(text))

    for r in range(1,4):
        print(grammar_three().parseString(text*r))

    print(grammar_four().parseString(text*2))

    text2 = ":20C::SEME//345678"
    print(grammar_five().parseString(text2))
    rr = grammar_six().parseString(text2)
    print(rr)

    text3 = [":20C:A", ":20C:B", ":20C:A"]
    for t in text3:
        print(grammar_seven().parseString(t))

    f_50h = grammar_50h()
    samples_50h = [""":50H:/344110001637
TESTAR00AXXX
Utrecht
Netherlands
""",
                   """:50H:/GB12SEPA12341234123412
ORDERING CUST NAME
ORDERING CUST ADDR LINE 1
ORDERING CUST ADDR LINE 2
ORDERING CUST ADDR LINE 3
"""]

    for sample_50h in samples_50h:
        r_50h = f_50h.parseString(sample_50h)
        print(r_50h)
        print(r_50h.OrderingCustomer)
        print(r_50h.Account)
        print(r_50h.NameAndAddress)

    g_bic  = grammar_bic()
    for bic in ["01234567890", "01234567"]:
        print(g_bic.parseString(bic))


