import copy


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r},{self.age!r})"


class Employee(Person):
    def __init__(self, name, age, ni):
        super().__init__(name, age)
        self.niNo = ni

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r},{self.age!r},{self.niNo!r})"


class EmployeeContained:
    def __init__(self, person: Person, ni: str):
        self.person = person
        self.niNo = ni

    def __repr__(self):
        return f"{self.__class__.__name__}({self.person!r},{self.niNo!r})"


def test_shallow():
    employee = Employee("Ketan", 21, "NH")
    employee_shallow = copy.copy(employee)
    employee_shallow.niNo = "MH"
    # Due to the object being simple no diff in shallow / deep
    assert employee.niNo != employee_shallow.niNo
    print(employee_shallow)
    print(employee)


def test_deep():
    employee = Employee("Ketan", 21, "NH")
    employee_shallow = copy.deepcopy(employee)
    employee_shallow.niNo = "MH"
    # Due to the object being simple no diff in shallow / deep
    assert employee.niNo != employee_shallow.niNo


def test_shallow_2():
    dict_persons = {"ketan": Person("Ketan",21), "notKetan" : Person("NotKetan",21)}
    dict_shallow_persons = copy.copy(dict_persons)
    dict_persons["ketan"] = Person("notKetan",21)
    assert(1==1)
