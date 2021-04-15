class MyClass:
    cv = 7

    def __init__(self):
        self.iv = 5

    def im(self):
        # Can modify instance and class variables
        self.iv = 5
        self.__class__.cv = 7
        print(self)
        return "im"

    @classmethod
    def cm(cls):
        # Can modify class variables
        # self.iv = 5
        cls.cv = 7
        print(cls)
        return "cm"

    @classmethod
    def make_A(cls):
        return cls()

    @classmethod
    def make_B(cls):
        return cls()

    @staticmethod
    def sm():
        # not able to modify class or instance variable
        return "sm"


def test_myclass():
    m = MyClass()
    assert(m.iv == 5)
    assert(m.cv == 7)
    assert(m.__class__.cv == 7)
    assert(m.im() == "im")

    assert(m.cm() == "cm")
    assert(MyClass.cm() == "cm")
    assert(MyClass.cv == 7)

    assert(m.sm() == "sm")
    assert(MyClass.sm() == "sm")

    assert(MyClass.make_B().cv == 7)
    assert(MyClass.make_A().cv == 7)
