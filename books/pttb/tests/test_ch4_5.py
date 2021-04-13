import pytest
from abc import ABCMeta, abstractmethod


class Base:
    def foo(self):
        raise NotImplementedError()

    def bar(self):
        raise NotImplementedError()


class Derived(Base):
    def foo(self):
        pass


class Base2(metaclass=ABCMeta):
    @abstractmethod
    def foo(self):
        pass

    @abstractmethod
    def bar(self):
        pass


class Derived2(Base2):
    def foo(self):
        pass


def test_base():
    with pytest.raises(NotImplementedError):
        b = Base()
        b.foo()


def test_derived():
    with pytest.raises(NotImplementedError):
        b = Derived()
        b.bar()


def test_base2():
    with pytest.raises(TypeError):
        base = Base2()


def test_derive2():
    with pytest.raises(TypeError):
        assert issubclass(Derived2, Base2)
        derived2 = Derived2()
