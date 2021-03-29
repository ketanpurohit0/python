from contextlib import contextmanager
from typing import TextIO, Optional, Iterator


@contextmanager
def managedFile(name: str) -> Iterator[Optional[TextIO]]:
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
    def __init__(self, name: str):
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

    def isClosed(self) -> bool:
        return True if not self.file else self.file and self.file.closed


class Indentor:
    level: int = 0

    def __init__(self):
        self.level = 0

    def __enter__(self):
        self.level += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.level -= 1

    def out(self, text: str) -> None:
        print(" " * self.level + text)
