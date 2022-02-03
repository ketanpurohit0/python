import json

def fixJson(json: str) -> str:
    fixJson = json.replace('{{','{"header":{')
    fixJson = fixJson.replace('SELL','"SELL"')
    fixJson = fixJson.replace('BUY','"BUY"')
    fixJson = fixJson.replace('"flags_":"{"','"flags_":{"')
    return fixJson

def filterIn(json: str) -> bool:
    return json.find("msgType_") > 0 and not json.find('"msgType_":11') > 0

if __name__ == '__main__':
    file = r"C:\Users\ketan\Downloads\pretrade_current.txt"
    with open(file, "r") as filereader:
        i = 0
        # use filereader as iterator, only keep lines with msgType_ in them.
        # this is to avoid 'spurious' entries (at least as I understand it presently)
        for line in filter(lambda x: x.find("msgType_") > 0,filereader):
            i+=1
            jsonStr = line[2:]
            fixedJson = fixJson(jsonStr)
            #print(fixedJson)
            jsonObj = json.loads(fixedJson)
            print(jsonObj["header"]["msgType_"])
        print(i)