import asyncio
import json
import time
from pathlib import Path
from typing import Dict, Any
import timeit

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
    files = [Path(f"rem{i}.txt") for i in range(30)]

    ts = time.perf_counter()
    t_ = 0
    for f in files:
        T = timeit.timeit('asyncio.run(write_to_file({"A": 2}, f))', number=100, globals=globals())
        t_ += T
    print(time.perf_counter() - ts, t_)

    ts = time.perf_counter()
    t_ = 0
    for f in files:
        T = timeit.timeit('asyncio.run(write_to_file_aiofiles({"A": 2}, f))', number=100, globals=globals())
        t_ += T
    print(time.perf_counter() - ts, t_)

    ts = time.perf_counter()
    t_ = 0
    T = timeit.timeit('asyncio.run(slow_method_wrapper(files))',number=100, globals=globals())
    t_ += T
    print(time.perf_counter() - ts, t_)

    ts = time.perf_counter()
    t_ = 0
    T = timeit.timeit('asyncio.run(slow_method_wrapper2(files))', number=100, globals=globals())
    t_ += T
    print(time.perf_counter() - ts, t_)

# 6.8296649 6.814284999999998
# 12.7765823 12.762742999999999
# 1.9098098000000014 1.9093440000000008
# 1.892768499999999 1.8770220999999978
