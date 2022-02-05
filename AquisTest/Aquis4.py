import json
import time
from multiprocessing import Process, JoinableQueue
from queue import Empty
from threading import Thread

import requests

from Aquis1 import filterIn, fixJson


def downloadFile(sourceFile: str, inboundQueue: JoinableQueue):
    streamingReadFromURL = requests.get(sourceFile, stream=True)
    for chunk in streamingReadFromURL.iter_lines(65536):
        inboundQueue.put_nowait(chunk)


def worker(name, queue: JoinableQueue):
    counter: int = 0
    msg8counter: int = 0
    msg12counter: int = 0
    msgOthercounter: int = 0
    notEmpty: bool = True

    while notEmpty and queue.qsize():
        try:
            jsonStr = queue.get(block=True, timeout=2.0)
            jsonStr = jsonStr.decode('ASCII')
            if filterIn(jsonStr):
                fixedJson = fixJson(jsonStr[2:])
                jsonObj = json.loads(fixedJson)
                msgType = jsonObj["header"]["msgType_"]
                # place the parsed json in relevant containers
                if msgType == 8:
                    msg8counter += 1
                elif msgType == 12:
                    msg12counter += 1
                else:
                    msgOthercounter += 1
            queue.task_done()
            counter += 1
            if counter % 10000 == 0:
                print(name, counter, msg8counter, msg12counter, msgOthercounter)
        except Empty:
            notEmpty = False

    print(f"{name} completed", counter, msg8counter, msg12counter, msgOthercounter)


def useProcess():
    sourceFile = r"https://aquis-public-files.s3.eu-west-2.amazonaws.com/market_data/current/pretrade_current.txt"
    inboundQueue = JoinableQueue()
    reader = Thread(target=downloadFile, args=(sourceFile, inboundQueue,))
    reader.start()

    workers = []
    for i in range(5):
        w = Process(target=worker, args=(f"worker-{i}", inboundQueue,))
        w.start()
        workers.append(w)

    time.sleep(5)
    print("*", inboundQueue.qsize())
    time.sleep(5)
    print("*", inboundQueue.qsize())

    reader.join()
    print("*", inboundQueue.qsize())
    inboundQueue.join()
    print("**", inboundQueue.qsize())
    map(lambda wi: wi.join(), workers)


if __name__ == '__main__':
    useProcess()
