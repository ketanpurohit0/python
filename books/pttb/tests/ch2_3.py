import os

import pytest


# context manager and with statement
class ManagedFile:
    def __init__(self, name):
        self.name = name
        self.file = None
        self.file_exists = False

    def __enter__(self):
        try:
            self.file = open(self.name, 'r')
            self.file_exists = True
        except FileNotFoundError:
            pass

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file_exists:
            self.file.close()

    def isClosed(self):
        return True if self.file_exists is False else self.file_exists and self.file.closed


def test_managed_file_without_cm():
    mf = ManagedFile("foo")
    # Not used in context manager, so the file is not opened
    # also "foo" does not exist any
    assert (mf.isClosed() is True)


def test_managed_file_with_cm_expect_True():
    os.getcwd()

    with ManagedFile("foo") as mf:
        # Reason foo does not exist
        assert (mf.isClosed() is True)

    assert (mf.isClosed())


def test_managed_file_with_cm_expect_mixed():
    with ManagedFile("ch2_3.py") as mf:
        assert (mf.isClosed() is False)

    assert (mf.isClosed())


@pytest.mark.parametrize("p1,p2",
                         [(1, 1), (2, 2), (3, 3)]
                         )
def test_param(p1, p2):
    assert (p1 == p2)
