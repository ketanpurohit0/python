import time
from multiprocessing import JoinableQueue
from queue import Empty
from threading import Thread
import requests
from AquisCommon import timing_val, SecuritiesDict, OrderStatisticsAggregator, writeResult, innerProcessor
import argparse


def downloadFile(sourceUrl: str, inboundQueue: JoinableQueue):
    """[Stream file from sourceUrl and place into queue for consumption]

    Args:
        sourceUrl (str): [The url of the source data]
        inboundQueue (JoinableQueue): [A queue from the multiprocessing module]
    """
    streamingReadFromURL = requests.get(sourceUrl, stream=True)
    for chunk in streamingReadFromURL.iter_lines(65536):
        inboundQueue.put_nowait(chunk)


def worker(queue: JoinableQueue, securitiesDictionary: SecuritiesDict,
           orderStatistics: OrderStatisticsAggregator):
    """[The worker that will run in a thread]

    Args:
        queue (JoinableQueue): [The queue from which items will be taken]
        securitiesDictionary (SecuritiesDict): [Contains securities data keyed by securityId]
        orderStatistics (OrderStatisticsAggregator): [Contains aggregated order data]
    """
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
    """[Stream process sourceFile into target using threads]

    Args:
        nThreads (int): [Number of worker threads to use]
        sourceUrl (str): [The url of the source data]
        targetTsvFile (str): [Target path of the output file]
    """
    inboundQueue = JoinableQueue()
    securitiesDictionary = SecuritiesDict()

    # aggregate orders in a collection here
    orderStatistics = OrderStatisticsAggregator()

    reader = Thread(target=downloadFile, args=(sourceUrl, inboundQueue,))
    reader.start()
    time.sleep(5)

    workers = []
    for _ in range(nThreads):
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

    parser = argparse.ArgumentParser()
    parser.add_argument("--sourceFile", type=str, help="path to source input file")
    parser.add_argument("--targetTsvFile", type=str, help="path to target tsv file")

    args = parser.parse_args()

    # sourceFile = r"https://aquis-public-files.s3.eu-west-2.amazonaws.com/market_data/current/pretrade_current.txt"
    # targetTsvFile = r".\useThreads.tsv"

    if args.sourceFile and args.targetTsvFile:
        nThreads = 2
        timer, _, _ = useThreads(nThreads, args.sourceFile, args.targetTsvFile)
        print("Time:", timer)
    else:
        import os
        fname = os.path.split(__file__)[-1]
        print(f'Missing arguments - usage: {fname} --sourceFile "path" --targetTsvFile  "path"')