import concurrent.futures
import os
import time
from concurrent.futures import ALL_COMPLETED
from pathlib import Path
from time import sleep


def expensive_func(file: Path, other: int) -> int:
    sleep(0.4)
    return other


if __name__ == "__main__":

    n_tasks = 4
    n_workers = os.cpu_count() * 2
    # sequential
    file = Path("a.txt")
    ts = time.perf_counter()
    for r in range(n_tasks):
        expensive_func(file, r)
    print(time.perf_counter() - ts)

    ts = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(expensive_func, file = file, other = r) for r in range(n_tasks)}

        # print(futures)

        concurrent.futures.wait(futures, timeout=None, return_when=ALL_COMPLETED)

        # print(futures)

        print(time.perf_counter() - ts)

        for f in futures:
            print(f.result())
