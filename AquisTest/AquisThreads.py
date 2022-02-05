import time
from multiprocessing import JoinableQueue
from queue import Empty
from threading import Thread
import requests
from AquisCommon import timing_val, SecuritiesDict, OrderStatisticsAggregator, writeResult, innerProcessor


def downloadFile(sourceUrl: str, inboundQueue: JoinableQueue):
    streamingReadFromURL = requests.get(sourceUrl, stream=True)
    for chunk in streamingReadFromURL.iter_lines(65536):
        inboundQueue.put_nowait(chunk)


def worker(queue: JoinableQueue, securitiesDictionary: SecuritiesDict,
           orderStatistics: OrderStatisticsAggregator):
    counter: int = 0
    notEmpty: bool = True

    while notEmpty and queue.qsize():
        try:
            jsonStr = queue.get(block=True, timeout=5.0)
            jsonStr = jsonStr.decode('ASCII')
            innerProcessor(jsonStr, securitiesDictionary, orderStatistics)
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

    writeResult(orderStatistics, securitiesDictionary, targetTsvFile)


if __name__ == '__main__':
    # use argparse here
    sourceFile = r"https://aquis-public-files.s3.eu-west-2.amazonaws.com/market_data/current/pretrade_current.txt"
    targetTsvFile = r".\useThreads.tsv"
    nThreads = 2
    timer, _, _ = useThreads(2, sourceFile, targetTsvFile)
    print("Time:", timer)
