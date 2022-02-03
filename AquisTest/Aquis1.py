import csv
import json
from typing import Any, Dict, Generator, List
import dataclasses


# Utilities
def fixJson(json: str) -> str:
    fixJson = json.replace('{{', '{"header":{')
    fixJson = fixJson.replace('SELL', '"SELL"')
    fixJson = fixJson.replace('BUY', '"BUY"')
    fixJson = fixJson.replace('"flags_":"{"', '"flags_":{"')
    return fixJson


def filterIn(json: str) -> bool:
    return json.find("msgType_") > 0 and not json.find('"msgType_":11') > 0


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
        oa.minSellPrice = min(oa.minSellPrice, price)
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


if __name__ == '__main__':
    # use argparse here
    sourceFile = r"C:\Users\ketan\Downloads\pretrade_current.txt"
    targetTsvFile = r".\pretrade_current.tsv"

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
    with open(targetTsvFile, "w", newline='') as filewriter:
        filewriter.write(" | ".join(OrderAggregate.header()) + "\n")
        tsvWriter = csv.writer(filewriter, delimiter="\t")
        for o in orderStatistics.collectAll():
            tsvWriter.writerow(o.toList(securitiesDictionary))
