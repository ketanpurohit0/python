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
    for f in files:
        asyncio.run(write_to_file({"A": 2}, f))
    print(time.perf_counter() - ts)

    ts = time.perf_counter()
    for f in files:
        asyncio.run(write_to_file_aiofiles({"A": 2}, f))
    print(time.perf_counter() - ts)

    ts = time.perf_counter()
    asyncio.run(slow_method_wrapper(files))
    print(time.perf_counter() - ts)

    ts = time.perf_counter()
    asyncio.run(slow_method_wrapper2(files))
    print(time.perf_counter() - ts)
