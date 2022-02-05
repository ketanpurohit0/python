import json
from AquisCommon import timing_val, SecuritiesDict, OrderStatisticsAggregator, filterIn, writeResult, fixJson


@timing_val
def useNaive(sourceFile: str, targetTsvFile: str) -> None:
    # create a lookup for securities built from messages of type 8
    # it has been observed that not all 'traded' have a type 8
    # hence referential integrity problem. See output
    securitiesDictionary = SecuritiesDict()

    # aggregate orders in a collection here
    orderStatistics = OrderStatisticsAggregator()

    # start reading the file, we only keep the message type 8 and 12
    # for purposes of this specific task
    with open(sourceFile, "r") as filereader:
        # use filereader as iterator, only keep lines with msgType_ in them.
        # this is to avoid 'spurious' entries (at least as I understand it presently)
        for line in filter(lambda x: filterIn(x), filereader):
            # remove the first two characters in source
            jsonStr = line[2:]
            # apply fixes for 'malformed' json
            fixedJson = fixJson(jsonStr)
            # parse the json
            jsonObj = json.loads(fixedJson)
            msgType = jsonObj["header"]["msgType_"]
            # place the parsed json in relevant containers
            if msgType == 8:
                securitiesDictionary.add(jsonObj)
            elif msgType == 12:
                orderStatistics.aggregate(jsonObj)

    # Write to target
    writeResult(orderStatistics, securitiesDictionary, targetTsvFile)


if __name__ == '__main__':
    # use argparse here
    sourceFile = r"C:\Users\ketan\Downloads\pretrade_current.txt"
    targetTsvFile = r".\naive.tsv"
    timer, _, _ = useNaive(sourceFile, targetTsvFile)
    print("Time:", timer)
