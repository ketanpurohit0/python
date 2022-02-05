import json
from datetime import time

import requests
import asyncio
from Aquis1 import filterIn, fixJson, SecuritiesDict, OrderStatisticsAggregator
from Aquis1 import OrderAggregate
from AquisCommon import timing_val
import csv
import time


async def innerWorker(jsonStr: str, securitiesDictionary: SecuritiesDict, orderStatistics: OrderStatisticsAggregator):
    if filterIn(jsonStr):
        fixedJson = fixJson(jsonStr[2:])
        jsonObj = json.loads(fixedJson)
        msgType = jsonObj["header"]["msgType_"]
        # place the parsed json in relevant containers
        if msgType == 8:
            securitiesDictionary.add(jsonObj)
        elif msgType == 12:
            orderStatistics.aggregate(jsonObj)


# worker on the queue, there would be multiple instances of these
async def worker(name, queue, securitiesDictionary: SecuritiesDict, orderStatistics: OrderStatisticsAggregator):
    while True:
        jsonStr = await queue.get()
        jsonStr = jsonStr.decode('ASCII')
        await innerWorker(jsonStr, securitiesDictionary, orderStatistics)
        queue.task_done()


async def main(sourceFile: str, securitiesDictionary: SecuritiesDict,
               orderStatistics: OrderStatisticsAggregator) -> None:
    # inboundQueue and associated workers
    inboundQueue = asyncio.Queue()
    tasks = []
    for i in range(3):
        task = asyncio.create_task(worker(f"worker{i}", inboundQueue, securitiesDictionary, orderStatistics))
        tasks.append(task)

    # do a streaming read
    streamingReadFromURL = requests.get(sourceFile, stream=True)
    for chunk in streamingReadFromURL.iter_lines(65536):
        inboundQueue.put_nowait(chunk)

    # informational
    # print(inboundQueue.qsize())

    # await inboundQueue to empty
    await inboundQueue.join()

    # cancel tasks as there will not be any more entries
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)

@timing_val
def useAsyncIo(sourceFile: str, targetTsvFile: str) -> None:
    # create a lookup for securities built from messages of type 8
    # it has been observed that not all 'traded' have a type 8
    # hence referential integrity problem. See output
    securitiesDictionary = SecuritiesDict()

    # aggregate orders in a collection here
    orderStatistics = OrderStatisticsAggregator()

    # launch
    asyncio.run(main(sourceFile, securitiesDictionary, orderStatistics))

    # write final results
    with open(targetTsvFile, "w", newline='') as filewriter:
        filewriter.write(" | ".join(OrderAggregate.header()) + "\n")
        tsvWriter = csv.writer(filewriter, delimiter="\t")
        for o in orderStatistics.collectAll():
            tsvWriter.writerow(o.toList(securitiesDictionary))


if __name__ == '__main__':
    sourceFile = r"https://aquis-public-files.s3.eu-west-2.amazonaws.com/market_data/current/pretrade_current.txt"
    targetTsvFile = r".\pretrade_current_ac3.tsv"
    timer, _, _ = useAsyncIo(sourceFile, targetTsvFile)
    print("Time:", timer)
