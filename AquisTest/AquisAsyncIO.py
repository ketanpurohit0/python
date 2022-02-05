import requests
import asyncio
from AquisCommon import timing_val, SecuritiesDict, OrderStatisticsAggregator, writeResult, innerProcessor


async def innerWorker(jsonStr: str, securitiesDictionary: SecuritiesDict, orderStatistics: OrderStatisticsAggregator):
    innerProcessor(jsonStr, securitiesDictionary, orderStatistics)


# worker on the queue, there would be multiple instances of these
async def worker(name, queue, securitiesDictionary: SecuritiesDict, orderStatistics: OrderStatisticsAggregator):
    while True:
        jsonStr = await queue.get()
        jsonStr = jsonStr.decode('ASCII')
        await innerWorker(jsonStr, securitiesDictionary, orderStatistics)
        queue.task_done()


async def main(nWorkers: int, sourceFile: str, securitiesDictionary: SecuritiesDict,
               orderStatistics: OrderStatisticsAggregator) -> None:
    # inboundQueue and associated workers
    inboundQueue = asyncio.Queue()
    tasks = []
    for i in range(nWorkers):
        task = asyncio.create_task(worker(f"worker{i}", inboundQueue, securitiesDictionary, orderStatistics))
        tasks.append(task)

    # do a streaming read
    await streamFileFromURL(inboundQueue, sourceFile)

    # informational
    # print(inboundQueue.qsize())

    # await inboundQueue to empty
    await inboundQueue.join()

    # cancel tasks as there will not be any more entries
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)


async def streamFileFromURL(inboundQueue, sourceFileURL):
    streamingReadFromURL = requests.get(sourceFileURL, stream=True)
    for chunk in streamingReadFromURL.iter_lines(65536):
        inboundQueue.put_nowait(chunk)


@timing_val
def useAsyncIo(nWorkers: int, sourceFile: str, targetTsvFile: str) -> None:
    # create a lookup for securities built from messages of type 8
    # it has been observed that not all 'traded' have a type 8
    # hence referential integrity problem. See output
    securitiesDictionary = SecuritiesDict()

    # aggregate orders in a collection here
    orderStatistics = OrderStatisticsAggregator()

    # launch
    asyncio.run(main(nWorkers, sourceFile, securitiesDictionary, orderStatistics))

    writeResult(orderStatistics, securitiesDictionary, targetTsvFile)


if __name__ == '__main__':
    # use argparse here
    sourceFile = r"https://aquis-public-files.s3.eu-west-2.amazonaws.com/market_data/current/pretrade_current.txt"
    targetTsvFile = r".\useAsyncIO.tsv"
    nWorkers = 2
    timer, _, _ = useAsyncIo(nWorkers, sourceFile, targetTsvFile)
    print("Time:", timer)
