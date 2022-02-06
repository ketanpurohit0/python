import json
from AquisCommon import SecuritiesDict, OrderStatisticsAggregator, innerProcessor, filterIn


def test_aggregator_buy_order():
    orderStatistics = OrderStatisticsAggregator()

    jsonStr = r'{"bookEntry_":{"securityId_":123,"side_":"BUY","quantity_":100,"price_":100,"orderId_":123}}'
    jsonObj = json.loads(jsonStr)
    orderStatistics.aggregate(jsonObj)
    o = orderStatistics.getAggregatedOrderForId(jsonObj["bookEntry_"]["securityId_"])
    assert o.totalBuyQty == 100
    assert o.totalSellQty == 0
    assert o.maxBuyPrice == 100
    assert o.minSellPrice == 0
    assert o.totalBuyOrders == 1
    assert o.totalSellOrders == 0
    assert o.weightedAverageBuyPrice() == 100
    assert o.weightedAverageSellPrice() == 0


def test_aggregator_sell_order():
    orderStatistics = OrderStatisticsAggregator()

    jsonStr = r'{"bookEntry_":{"securityId_":123,"side_":"SELL","quantity_":200,"price_":200,"orderId_":123}}'
    jsonObj = json.loads(jsonStr)
    orderStatistics.aggregate(jsonObj)
    o = orderStatistics.getAggregatedOrderForId(jsonObj["bookEntry_"]["securityId_"])
    assert o.totalBuyQty == 0
    assert o.totalSellQty == 200
    assert o.maxBuyPrice == 0
    assert o.minSellPrice == 200
    assert o.totalBuyOrders == 0
    assert o.totalSellOrders == 1
    assert o.weightedAverageBuyPrice() == 0
    assert o.weightedAverageSellPrice() == 200


def test_aggregator_mixed_orders():
    orderStatistics = OrderStatisticsAggregator()

    orders = [r'{"bookEntry_":{"securityId_":123,"side_":"BUY","quantity_":100,"price_":100,"orderId_":123}}',
              r'{"bookEntry_":{"securityId_":123,"side_":"SELL","quantity_":200,"price_":200,"orderId_":123}}'
              ]

    for order in orders:
        jsonObj = json.loads(order)
        orderStatistics.aggregate(jsonObj)

    o = orderStatistics.getAggregatedOrderForId(123)
    assert o.totalBuyQty == 100
    assert o.totalSellQty == 200
    assert o.maxBuyPrice == 100
    assert o.minSellPrice == 200
    assert o.totalBuyOrders == 1
    assert o.totalSellOrders == 1
    assert o.weightedAverageBuyPrice() == 100
    assert o.weightedAverageSellPrice() == 200


def test_logic():
    # Test consists of two files, one containing the test input in the manner
    # matching the sample file (changed ids, names) and with a csv file consisting
    # of expected values. There is also 'bad' json, which will be skipped over

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
            # compare expected v with actual v
            av = resultsDict[k]
            assert len(ev) == len(av), "Different lengths of expected values and actual values"
            outcome = [str(ev[i]) == str(av[i]) for i in range(len(ev))]
            assert all(outcome), f"{k} has a difference in one of the outcomes\n{ev}\n{av}"
        else:
            assert False, f"{k} is in expected result but not in actual result"
