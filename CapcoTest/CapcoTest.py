import asyncio
import json
from pathlib import Path
from typing import Dict, Any

import aiofiles


async def write_to_file(a:Dict[str,Any], file: Path):
     with  open(file, "w", encoding="utf-8") as ff:
        json.dump(a, ff,indent=4)
        ff.close()


async def write_to_file_aiofiles(a: Dict[str, Any], file: Path):
    with  aiofiles.open(file, "w", encoding="utf-8") as ff:
        json.dump(a, ff, indent=4)
        await ff.close()

if __name__ == "__main__":
    files = [Path( f"rem{i}.txt") for i in range(30)]

    # for f in files:
    #     asyncio.run(write_to_file({"A" : 2}, f))

    for f in files:
        asyncio.run(write_to_file_aiofiles({"A" : 2}, f))
