import unittest

import pytest
from hypothesis import given, example, settings
from hypothesis.strategies import text, integers, lists, tuples


def test_answer():
    assert True  # to see what was printed


def foo():
    return 1


@given(s=text("A-Z", min_size=10, max_size=20))
@example(s="Ketan")
def test_text(s):
    print(s)
    assert True


@given(s=integers(min_value=10, max_value=20))
def test_integers(s):
    print(s)
    assert True


@given(s=lists(integers(min_value=10, max_value=200), min_size=10))
def test_lists_integers(s):
    print(s)
    assert True


@given(tuples(integers(), text("Ketan")))
def test_tuples(s):
    print(s)


def test_stateful():
    from StateFullTesting import DieHardProblem
    DieHardTest = DieHardProblem.TestCase


def test_heap():
    from StateFullTesting import HeapMachine
    HeapTest = HeapMachine.TestCase


def test_database():
    from StateFullTesting import DatabaseComparison
    TestDBComparison = DatabaseComparison.TestCase
