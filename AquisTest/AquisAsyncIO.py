import requests
import asyncio
from AquisCommon import timing_val, SecuritiesDict, OrderStatisticsAggregator, writeResult, innerProcessor
import argparse


async def innerWorker(jsonStr: str, securitiesDictionary: SecuritiesDict, orderStatistics: OrderStatisticsAggregator):
    innerProcessor(jsonStr, securitiesDictionary, orderStatistics)


# worker on the queue, there would be multiple instances of these
async def worker(name: str, queue: asyncio.Queue, securitiesDictionary: SecuritiesDict, orderStatistics: OrderStatisticsAggregator):
    """[Worker for consuming the queue]

    Args:
        name ([str]): [Worker name]
        queue ([asyncio.Queue]): [Queue that will be processed]
        securitiesDictionary (SecuritiesDict): [Contains securities data keyed by securityId]
        orderStatistics (OrderStatisticsAggregator): [Contains aggregated order data]
    """
    while True:
        jsonStr = await queue.get()
        jsonStr = jsonStr.decode('ASCII')
        await innerWorker(jsonStr, securitiesDictionary, orderStatistics)
        queue.task_done()


async def main(nWorkers: int, sourceFile: str, securitiesDictionary: SecuritiesDict,
               orderStatistics: OrderStatisticsAggregator) -> None:
    """[The main co-routine for processing the file]

    Args:
        nWorkers (int): [number of worker tasks]
        sourceFile (str): [The url of the source data]
        securitiesDictionary (SecuritiesDict): [Contains securities data keyed by securityId]
        orderStatistics (OrderStatisticsAggregator): [Contains aggregated order data]
    """
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


async def streamFileFromURL(inboundQueue :asyncio.Queue, sourceFileURL):
    """[Will stream data from the given URL and place each line in a queue]

    Args:
        inboundQueue ([asyncio.Queue]): [A queue from the asyncio package]
        sourceFileURL ([str]): [The url of the source data]
    """
    streamingReadFromURL = requests.get(sourceFileURL, stream=True)
    for chunk in streamingReadFromURL.iter_lines(65536):
        inboundQueue.put_nowait(chunk)


@timing_val
def useAsyncIo(nWorkers: int, sourceFile: str, targetTsvFile: str) -> None:
    """[Stream process sourceFile into target using asyncio worker]

    Args:
        nWorkers (int): [Number of worker (tasks)]
        sourceFile (str): [URL of the source, this file will be streamed.]
        targetTsvFile (str): [Target path of the output file]
    """
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

    parser = argparse.ArgumentParser()
    parser.add_argument("--sourceFile", type=str, help="path to source input file")
    parser.add_argument("--targetTsvFile", type=str, help="path to target tsv file")

    args = parser.parse_args()

    # sourceFile = r"https://aquis-public-files.s3.eu-west-2.amazonaws.com/market_data/current/pretrade_current.txt"
    # targetTsvFile = r".\useAsyncIO.tsv"

    if args.sourceFile and args.targetTsvFile:
        nWorkers = 2
        timer, _, _ = useAsyncIo(nWorkers, args.sourceFile, args.targetTsvFile)
        print("Time:", timer)
    else:
        import os
        fname = os.path.split(__file__)[-1]
        print(f'Missing arguments - usage: {fname} --sourceFile "path" --targetTsvFile  "path"')
