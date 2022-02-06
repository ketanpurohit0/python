import logging
import time
import csv
from json import JSONDecodeError
from typing import Any, Dict, Generator, List
import dataclasses
import json

logger = logging.getLogger(__name__)
# Decorator for timing
def timing_val(func):
    def wrapper(*arg, **kw):
        """source: http://www.daniweb.com/code/snippet368.html"""
        t1 = time.time()
        res = func(*arg, **kw)
        t2 = time.time()
        return (t2 - t1), res, func.__name__

    return wrapper


# Utility for fixing bad json
def fixJson(jsonStr: str) -> str:
    fixJson = jsonStr.replace('{{', '{"header":{')
    fixJson = fixJson.replace('SELL,', '"SELL",')
    fixJson = fixJson.replace('BUY,', '"BUY",')
    fixJson = fixJson.replace('"flags_":"{"', '"flags_":{"')
    return fixJson


# Utility for filtering only messages of interest
def filterIn(jsonStr: str) -> bool:
    return jsonStr.find("msgType_") > 0 and not jsonStr.find('"msgType_":11') > 0


# Contains securities indexed by securityId
class SecuritiesDict:
    def __init__(self):
        self.securitiesDictionary: Dict[int, Any] = {}

    def add(self, jsonObj: Any) -> None:
        # for clarity rather than performance
        securityId = jsonObj["security_"]["securityId_"]
        self.securitiesDictionary[securityId] = jsonObj["security_"]

    def getSecurityJson(self, securityId: int) -> Any:
        return self.securitiesDictionary[securityId]

    def getSecurityAttribute(self, securityId: int, attribute: str):
        return self.securitiesDictionary[securityId][attribute] if self.contains(
            securityId) else f"*SECID({securityId}) MISSING*"

    def contains(self, securityId: int):
        return securityId in self.securitiesDictionary

    def size(self):
        return len(self.securitiesDictionary)

    def __repr__(self):
        return f"{self.__class__.__name__} records {len(self.securitiesDictionary)}"


@dataclasses.dataclass
class OrderAggregate:
    accumulateBuys: float = 0
    accumulateSells: float = 0

    securityId: int = 0
    totalBuyOrders: int = 0
    totalSellOrders: int = 0
    totalBuyQty: int = 0
    totalSellQty: int = 0
    maxBuyPrice: float = 0
    minSellPrice: float = 0

    @classmethod
    def header(cls):
        return ["ISIN", "Currency", "Total Buy Count", "Total Sell Count", "Total Buy Quantity", "Total Sell Quantity",
                "Weighted Average Buy Price", "Weighted Average Sell Price", "Max Buy Price", "Min Sell Price"]

    # calculated fields from accumulated
    def weightedAverageBuyPrice(self):
        return 0 if self.totalBuyQty == 0 else self.accumulateBuys / self.totalBuyQty

    def weightedAverageSellPrice(self):
        return 0 if self.totalSellQty == 0 else self.accumulateSells / self.totalSellQty

    # return a list of attributes of interest for express purpose
    # of writing a delimited file
    def toList(self, securitiesDict: SecuritiesDict) -> List[Any]:
        isin = securitiesDict.getSecurityAttribute(self.securityId, "isin_")
        currency = securitiesDict.getSecurityAttribute(self.securityId, "currency_")
        return [isin,
                currency,
                self.totalBuyOrders,
                self.totalSellOrders,
                self.totalBuyQty,
                self.totalSellQty,
                self.weightedAverageBuyPrice(),
                self.weightedAverageSellPrice(),
                self.maxBuyPrice,
                self.minSellPrice
                ]


# Contains order aggregated by securityId
class OrderStatisticsAggregator:

    def __init__(self):
        # securityId_ -> statistics
        self.orders: Dict[int, OrderAggregate] = {}  # = defaultdict(default_factory=OrderAggregate())
        self.accumulatorFunction: Dict[str, Any] = {"BUY": self.accumulateBuy, "SELL": self.accumulateSell}

    def accumulateBuy(self, oa: OrderAggregate, jsonObj: Any) -> OrderAggregate:
        price = jsonObj["bookEntry_"]["price_"]
        quantity = jsonObj["bookEntry_"]["quantity_"]
        oa.totalBuyOrders += 1
        oa.totalBuyQty += quantity
        oa.maxBuyPrice = max(oa.maxBuyPrice, price)
        oa.accumulateBuys += quantity * price
        return oa

    def accumulateSell(self, oa: OrderAggregate, jsonObj: Any) -> OrderAggregate:
        price = jsonObj["bookEntry_"]["price_"]
        quantity = jsonObj["bookEntry_"]["quantity_"]
        oa.totalSellOrders += 1
        oa.totalSellQty += quantity
        oa.minSellPrice = price if (oa.minSellPrice == 0) else min(oa.minSellPrice, price)
        oa.accumulateSells += quantity * price
        return oa

    def aggregate(self, jsonObj: Any) -> None:
        securityId = jsonObj["bookEntry_"]["securityId_"]
        direction = jsonObj["bookEntry_"]["side_"]
        accumulatorFn = self.accumulatorFunction[direction]
        oa = self.getAggregatedOrderForId(securityId)
        self.orders[securityId] = accumulatorFn(oa, jsonObj)

    def getAggregatedOrderForId(self, securityId: int) -> OrderAggregate:
        return self.orders.get(securityId, OrderAggregate(securityId=securityId))

    def collectAll(self) -> Generator:
        for o in self.orders.values():
            yield o


def innerProcessor(jsonStr: str, securitiesDictionary: SecuritiesDict, orderStatistics: OrderStatisticsAggregator):
    if filterIn(jsonStr):
        fixedJson = fixJson(jsonStr[2:])
        try:
            jsonObj = json.loads(fixedJson)
            msgType = jsonObj["header"]["msgType_"]
            # place the parsed json in relevant containers
            if msgType == 8:
                securitiesDictionary.add(jsonObj)
            elif msgType == 12:
                orderStatistics.aggregate(jsonObj)

        except JSONDecodeError as jsonError:
            logger.error(f"JSONDecodeError {jsonError.msg}")


def writeResult(orderStatistics: OrderStatisticsAggregator, securitiesDictionary: SecuritiesDict, targetTsvFile):
    with open(targetTsvFile, "w", newline='') as filewriter:
        filewriter.write(" | ".join(OrderAggregate.header()) + "\n")
        tsvWriter = csv.writer(filewriter, delimiter="\t")
        for o in orderStatistics.collectAll():
            tsvWriter.writerow(o.toList(securitiesDictionary))
