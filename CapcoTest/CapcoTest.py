import asyncio
import json
import time
from pathlib import Path
from typing import Dict, Any
import timeit
import heapq

import aiofiles


async def write_to_file(a: Dict[str, Any], file: Path):
    await slow_method(a, file)


async def slow_method(a, file):
    with open(file, "w", encoding="utf-8") as ff:
        json.dump(a, ff, indent=4)
        ff.close()


async def slow_method_wrapper(files):
    for f in files:
        await slow_method({"A": 2}, f)

async def slow_method_wrapper2(files):
    list_of_funcs = (slow_method({"A":2}, f) for f in files)
    await asyncio.gather(*list_of_funcs)



async def write_to_file_aiofiles(a: Dict[str, Any], file: Path):
    async with aiofiles.open(file, "w", encoding="utf-8") as ff:
        json.dump(a, ff, indent=4)
        await ff.close()




if __name__ == "__main__":

    track = []
    n_tests = 100

    files = [Path(f"rem{i}.txt") for i in range(30)]

    ts = time.perf_counter()
    t_ = 0
    for f in files:
        T = timeit.timeit('asyncio.run(write_to_file({"A": 2}, f))', number=n_tests, globals=globals())
        t_ += T
    print(time.perf_counter() - ts, t_)
    track.append(t_)

    ts = time.perf_counter()
    t_ = 0
    for f in files:
        T = timeit.timeit('asyncio.run(write_to_file_aiofiles({"A": 2}, f))', number=n_tests, globals=globals())
        t_ += T
    print(time.perf_counter() - ts, t_)
    track.append(t_)

    ts = time.perf_counter()
    t_ = 0
    T = timeit.timeit('asyncio.run(slow_method_wrapper(files))',number=n_tests, globals=globals())
    t_ += T
    print(time.perf_counter() - ts, t_)
    track.append(t_)


    ts = time.perf_counter()
    t_ = 0
    T = timeit.timeit('asyncio.run(slow_method_wrapper2(files))', number=n_tests, globals=globals())
    t_ += T
    print(time.perf_counter() - ts, t_)
    track.append(t_)

    h = heapq.heapify(track)
    print(track)


# 127.629272 127.52956860000002
# 157.28519150000002 157.18686030000015
# 18.23259059999998 18.229185400000006
# 18.10036980000001 17.953597000000002
# [17.953597000000002, 127.52956860000002, 18.229185400000006, 157.18686030000015]

