from contextlib import contextmanager


@contextmanager
def managedFile(name: str):
    f = None
    try:
        f = open(name, "r")
        yield f
    except FileNotFoundError:
        yield f
    finally:
        if f:
            f.close()


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