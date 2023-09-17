import asyncio
import time
import httpx
import aiohttp
import requests

async def main():
    # Create clients for both the library
    httpx_client = httpx.AsyncClient()
    aiohttp_client = aiohttp.ClientSession()

    n = 100

    try:
        # Send 100 asynchronous GET requests using HTTPX
        start_time = time.perf_counter()
        tasks = [ httpx_client.get("https://example.com") for _ in range(n)]
        f_httpx = await asyncio.gather(*tasks)
        end_time = time.perf_counter()
        print(f"HTTPX: {end_time - start_time:.2f} seconds")

        # Send 100 asynchronous GET requests using AIOHTTP
        start_time = time.perf_counter()
        tasks = [aiohttp_client.get("https://example.com") for _ in range(n)]
        f_aio = await asyncio.gather(*tasks)
        end_time = time.perf_counter()
        print(f"AIOHTTP: {end_time - start_time:.2f} seconds")
    finally:
        # Close client sessions
        await aiohttp_client.close()
        await httpx_client.aclose()

asyncio.run(main())
