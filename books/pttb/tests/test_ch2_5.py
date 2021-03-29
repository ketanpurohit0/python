# string formatting
from string import Template


def test_sub():
    s: str = "$greeting $name"
    r = Template(s).substitute(greeting="Hello", name="World")
    assert(r == "Hello World")

    kw = {"greeting": "Hello", "name": "World"}
    r = Template(s).substitute(kw)
    assert (r == "Hello World")