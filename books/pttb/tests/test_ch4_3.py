import pytest


def validate(name):
    if len(name) < 10:
        raise ValueError
    pass


class ValidationError(ValueError):
    pass


def validate_2(name):
    if len(name) < 10:
        raise ValidationError(name)
    pass


def test_validate():
    with pytest.raises(ValueError):
        validate("foo")


def test_validate_2():
    with pytest.raises(ValidationError):
        validate_2("foo")


def test_validate_2_1():
    validate_2("foo")
