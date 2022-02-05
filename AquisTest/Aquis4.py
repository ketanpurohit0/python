import json
import time
from multiprocessing import JoinableQueue
from queue import Empty
from threading import Thread
import csv
import requests
from Aquis1 import filterIn, fixJson, SecuritiesDict, OrderStatisticsAggregator, OrderAggregate
from AquisCommon import timing_val


def downloadFile(sourceUrl: str, inboundQueue: JoinableQueue):
    streamingReadFromURL = requests.get(sourceUrl, stream=True)
    for chunk in streamingReadFromURL.iter_lines(65536):
        inboundQueue.put_nowait(chunk)


def innerWorker(jsonStr: str, securitiesDictionary: SecuritiesDict, orderStatistics: OrderStatisticsAggregator):
    if filterIn(jsonStr):
        fixedJson = fixJson(jsonStr[2:])
        jsonObj = json.loads(fixedJson)
        msgType = jsonObj["header"]["msgType_"]
        # place the parsed json in relevant containers
        if msgType == 8:
            securitiesDictionary.add(jsonObj)
        elif msgType == 12:
            orderStatistics.aggregate(jsonObj)


def worker(queue: JoinableQueue, securitiesDictionary: SecuritiesDict,
           orderStatistics: OrderStatisticsAggregator):
    counter: int = 0
    notEmpty: bool = True

    while notEmpty and queue.qsize():
        try:
            jsonStr = queue.get(block=True, timeout=5.0)
            jsonStr = jsonStr.decode('ASCII')
            innerWorker(jsonStr, securitiesDictionary, orderStatistics)
            queue.task_done()
            counter += 1
        except Empty:
            notEmpty = False


@timing_val
def useThreads(nThreads: int, sourceUrl: str, targetTsvFile: str) -> None:
    inboundQueue = JoinableQueue()
    securitiesDictionary = SecuritiesDict()

    # aggregate orders in a collection here
    orderStatistics = OrderStatisticsAggregator()

    reader = Thread(target=downloadFile, args=(sourceUrl, inboundQueue,))
    reader.start()
    time.sleep(5)

    workers = []
    for i in range(nThreads):
        w = Thread(target=worker, args=(inboundQueue, securitiesDictionary, orderStatistics,))
        w.start()
        workers.append(w)

    # wait for threads to complete
    reader.join()
    map(lambda wi: wi.join(), workers)
    inboundQueue.join()

    # write final results
    with open(targetTsvFile, "w", newline='') as filewriter:
        filewriter.write(" | ".join(OrderAggregate.header()) + "\n")
        tsvWriter = csv.writer(filewriter, delimiter="\t")
        for o in orderStatistics.collectAll():
            tsvWriter.writerow(o.toList(securitiesDictionary))


if __name__ == '__main__':
    # use argparse here
    sourceFile = r"https://aquis-public-files.s3.eu-west-2.amazonaws.com/market_data/current/pretrade_current.txt"
    targetTsvFile = r".\useThreads.tsv"
    timer, _, _ = useThreads(4, sourceFile, targetTsvFile)
    print("Time:", timer)
