import os


# context manager and with statement
class ManagedFile:
    def __init__(self, name):
        self.name = name
        self.file = None

    def __enter__(self):
        try:
            self.file = open(self.name, 'r')
        except FileNotFoundError:
            pass

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()

    def isClosed(self):
        return True if not self.file else self.file and self.file.closed


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
    with ManagedFile("test_ch2_3.py") as mf:
        assert (mf.isClosed() is False)

    assert (mf.isClosed())
