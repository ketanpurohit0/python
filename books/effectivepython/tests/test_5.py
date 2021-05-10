from urllib.parse import parse_qs


def test_parse():
    values = parse_qs("a&b=1&c=5&d=ketan&a=3",keep_blank_values=True)
    print(values)
    r = values.get('a',['']) or False


