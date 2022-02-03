import json
from typing import Any, Dict, Generator
import dataclasses
from string import Template

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
        return self.securitiesDictionary[securityId][attribute] if self.contains(securityId) else f"*SECID({securityId}) MISSING*"

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

    def weightedAverageBuyPrice(self):
        return 0 if self.totalBuyQty == 0 else self.accumulateBuys / self.totalBuyQty

    def weightedAverageSellPrice(self):
        return 0 if self.totalSellQty == 0 else self.accumulateSells / self.totalSellQty

    def toList(self, securitiesDict: SecuritiesDict) -> str:
        isin = securitiesDictionary.getSecurityAttribute(self.securityId, "isin_")
        currency = securitiesDictionary.getSecurityAttribute(self.securityId, "currency_")
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
class OrderStatisticsDict:

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
            #print(o.securityId, o.totalSellOrders, o.totalSellQty, o.accumulateSells, o.weightedAverageSellPrice())
        #return []
        # print(self.orders.keys())


if __name__ == '__main__':
    file = r"C:\Users\ketan\Downloads\pretrade_current.txt"

    securitiesDictionary = SecuritiesDict()
    orderStatistics = OrderStatisticsDict()

    with open(file, "r") as filereader:
        i = 0
        # use filereader as iterator, only keep lines with msgType_ in them.
        # this is to avoid 'spurious' entries (at least as I understand it presently)
        for line in filter(lambda x: filterIn(x), filereader):
            i += 1
            jsonStr = line[2:]
            fixedJson = fixJson(jsonStr)
            jsonObj = json.loads(fixedJson)
            msgType = jsonObj["header"]["msgType_"]
            if msgType == 8:
                securitiesDictionary.add(jsonObj)
            elif msgType == 12:
                orderStatistics.aggregate(jsonObj)

        #print(i, securitiesDictionary, securitiesDictionary.getSecurityAttribute(3450, "isin_"))
        # print(orderStatistics.getAggregateFor(123).totalSellOrders)
    for o in orderStatistics.collectAll():
        print(o.toList(securitiesDictionary))
