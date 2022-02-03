import json
from typing import Any, Dict


def fixJson(json: str) -> str:
    fixJson = json.replace('{{', '{"header":{')
    fixJson = fixJson.replace('SELL', '"SELL"')
    fixJson = fixJson.replace('BUY', '"BUY"')
    fixJson = fixJson.replace('"flags_":"{"', '"flags_":{"')
    return fixJson


def filterIn(json: str) -> bool:
    return json.find("msgType_") > 0 and not json.find('"msgType_":11') > 0


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
        return self.securitiesDictionary[securityId][attribute]

    def __repr__(self):
        return f"{self.__class__.__name__} records {len(self.securitiesDictionary)}"


class OrderStatisticsDict:
    def __init__(self):
        # securityId_ -> statistics
        self.sells : Dict[int, int] = {}
        self.buys : Dict[int, int] = {}

    def aggregate(self, jsonObj: Any) -> None:
        pass

    def collect(self)-> list:
        pass

if __name__ == '__main__':
    file = r"C:\Users\ketan\Downloads\pretrade_current.txt"

    securitiesDictionary = SecuritiesDict()

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
                pass

        print(i, securitiesDictionary, securitiesDictionary.getSecurityAttribute(3450, "isin_"))
