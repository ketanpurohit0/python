class MyClass:
    cv = 7

    def __init__(self):
        self.iv = 5

    def im(self):
        print(self)
        return "im"

    @classmethod
    def cm(cls):
        print(cls)
        return "cm"

    @staticmethod
    def sm():
        return "sm"


def test_myclass():
    m = MyClass()
    assert(m.iv == 5)
    assert(m.cv == 7)
    assert(m.__class__.cv == 7)
    assert(m.im() == "im")

    m.cm()
    assert(MyClass.cm() == "cm")

    m.sm()
    assert(MyClass.sm() == "sm")
