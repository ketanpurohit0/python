from AquisCommon import timing_val, SecuritiesDict, OrderStatisticsAggregator, filterIn, writeResult, innerProcessor


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
            innerProcessor(line, securitiesDictionary, orderStatistics)

    # Write to target
    writeResult(orderStatistics, securitiesDictionary, targetTsvFile)


if __name__ == '__main__':
    # use argparse here
    sourceFile = r"C:\Users\ketan\Downloads\pretrade_current.txt"
    targetTsvFile = r".\naive.tsv"
    timer, _, _ = useNaive(sourceFile, targetTsvFile)
    print("Time:", timer)
