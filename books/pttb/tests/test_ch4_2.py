# String conversion

class Car:
    def __init__(self, color, mileage):
        self.color = color
        self.mileage = mileage

    def __str__(self):
        # used by print or convert to string
        return f"{self.color}, {self.mileage}"

    def __repr__(self):
        # used by object inspector
        return f"{__name__}.{self.__class__.__name__}({self.color!r}, {self.mileage!r})"


def test_car():
    c = Car("Red", 20000)
    print(repr(c))
    print(c)
    print(f"{c!r}")
    print(str(c))


def test_date():
    import datetime
    today = datetime.date.today()
    print(str(today))
    print(repr(today))
