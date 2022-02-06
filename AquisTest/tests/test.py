import pytest
from AquisCommon import SecuritiesDict, OrderStatisticsAggregator, innerProcessor, filterIn


def test_logic():
    # Test consists of two files, one containing the test input in the manner
    # matching the sample file (changed ids, names) and with a csv file consisting
    # of expected values.
    
    testInputFile = r"../resources/testInput.txt"
    expectedResultFile = r"../resources/expectedTestOutput.csv"
    securitiesDictionary = SecuritiesDict()

    # aggregate orders in a collection here
    orderStatistics = OrderStatisticsAggregator()

    # start reading the file, we only keep the message type 8 and 12
    # for purposes of this specific task
    with open(testInputFile, "r") as filereader:
        # use filereader as iterator, only keep lines with msgType_ in them.
        # this is to avoid 'spurious' entries (at least as I understand it presently)
        for line in filter(lambda x: filterIn(x), filereader):
            innerProcessor(line, securitiesDictionary, orderStatistics)

    resultsDict = {}
    for o in orderStatistics.collectAll():
        asList = o.toList(securitiesDictionary)
        resultsDict[asList[0]] = asList[1:]

    # load the expected results file
    expectedResultsDict = {}
    with open(expectedResultFile, newline='') as csvfile:
        for r in csvfile.readlines():
            asList = r.rstrip().split(',')
            if asList[0] != 'ISIN':
                expectedResultsDict[asList[0]] = asList[1:]

    for k, ev in expectedResultsDict.items():
        if k in resultsDict:
            # compare e-xpected v with a-ctual v
            av = resultsDict[k]
            assert len(ev) == len(av), "Different lengths of expected values and actual values"
            outcome = [str(ev[i]) == str(av[i]) for i in range(len(ev))]
            assert all(outcome), f"{k} has a difference in one of the outcomes"
        else:
            assert False, f"{k} is in expected result but not in actual result"
