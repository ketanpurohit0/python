# Underscores, Dunders and more

def external_func():
    pass


# will not be visible if import *
def _internal_func():
    pass


def param_is_reserved_word(class_):
    return class_


class Test:
    def __init__(self, first: str, second: str):
        self.first = first
        self._second = second
        self.__third = "foo"

    def get_third(self):
        return self.__third


class ExtendedTest(Test):
    def __init__(self, first: str, second: str):
        self.first = first
        self._second = second
        self.__third = "foo"

    def get_third(self):
        return self.__third
