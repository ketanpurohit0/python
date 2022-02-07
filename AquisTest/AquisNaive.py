import argparse
from AquisCommon import timing_val, SecuritiesDict, OrderStatisticsAggregator, filterIn, writeResult, innerProcessor


@timing_val
def useNaive(sourceFile: str, targetTsvFile: str) -> None:
    """[Process the input source files]

    Args:
        sourceFile (str): [Path to the location of the input data]
        targetTsvFile (str): [Path to the location of the target data]
    """
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
    """[Entry point for processing the files, sources the data and aggregates it]
    """
    # use argparse here

    parser = argparse.ArgumentParser()
    parser.add_argument("--sourceFile", type=str, help="path to source input file")
    parser.add_argument("--targetTsvFile", type=str, help="path to target tsv file")

    args = parser.parse_args()

    # sourceFile = r"C:\Users\ketan\Downloads\pretrade_current.txt"
    # targetTsvFile = r".\useNaive.tsv"

    if args.sourceFile and args.targetTsvFile:
        timer, _, _ = useNaive(args.sourceFile, args.targetTsvFile)
        print("Time:", timer)
    else:
        import os
        fname = os.path.split(__file__)[-1]
        print(f'Missing arguments - usage: {fname} --sourceFile "path" --targetTsvFile  "path"')
