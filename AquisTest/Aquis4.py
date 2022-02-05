import asyncio
import time

import requests
from threading import Thread
from multiprocessing import Process ,JoinableQueue, Pool
from queue import Empty


def downloadFile(sourceFile: str, inboundQueue : JoinableQueue):
    streamingReadFromURL = requests.get(sourceFile, stream=True)
    for chunk in streamingReadFromURL.iter_lines(65536):
        inboundQueue.put_nowait(chunk)


def worker(name, queue: JoinableQueue):
    counter : int = 0
    notEmpty : bool = True
    while notEmpty and queue.qsize():
        try:
            jsonStr = queue.get(block=True, timeout=2.0)
            jsonStr = jsonStr.decode('ASCII')
            queue.task_done()
            counter+=1
            if counter % 10000 == 0:
                print(counter, name)
        except Empty:
            notEmpty = False

    print(counter, name)




def useProcess():
    sourceFile = r"https://aquis-public-files.s3.eu-west-2.amazonaws.com/market_data/current/pretrade_current.txt"
    inboundQueue = JoinableQueue()
    reader = Thread(target=downloadFile, args=(sourceFile, inboundQueue,))
    reader.start()

    workers = []
    for i in range(5):
        w = Process(target=worker, args=(f"worker-{i}",inboundQueue,))
        w.start()
        workers.append(w)




    time.sleep(5)
    print("*",inboundQueue.qsize())
    time.sleep(5)
    print("*",inboundQueue.qsize())

    reader.join()
    print("*",inboundQueue.qsize())
    inboundQueue.join()
    print("**",inboundQueue.qsize())
    map(lambda w: w.join(), workers)


if __name__ == '__main__':
    useProcess()
