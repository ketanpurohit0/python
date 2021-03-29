# context manager and with statement
import src.ch2_3 as mfi


def test_managed_file_without_cm():
    mf = mfi.ManagedFile("foo")
    # Not used in context manager, so the file is not opened
    # also "foo" does not exist any
    assert (mf.isClosed() is True)


def test_managed_file_with_cm_expect_True():
    with mfi.ManagedFile("foo") as mf:
        # Reason foo does not exist
        assert (mf.isClosed() is True)

    assert (mf.isClosed())


def test_managed_file_with_cm_expect_mixed():
    with mfi.ManagedFile("test_ch2_3.py") as mf:
        assert (mf.isClosed() is False)

    assert (mf.isClosed())


def test_decorated_managedFile_with_cm_expect_True():
    with mfi.managedFile("foo") as mf:
        # Reason foo does not exist
        assert (not mf)


def test_decorated_managedFile_expect_mixed():
    with mfi.managedFile("test_ch2_3.py") as mf:
        assert (mf and not mf.closed)

    assert (mf and mf.closed)