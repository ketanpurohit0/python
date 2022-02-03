from pyspark.sql import SparkSession
from Aquis1 import filterIn, fixJson
import json
from pyspark.sql.functions import sum as _sum, min as _min, max as _max
from pyspark.sql.functions import count, col, avg


def fixAndSplitInputFile(sourceFile: str, messageEightFile: str, messageTwelveFile: str) -> None:
    with open(messageEightFile, "w") as message8writer:
        with open(messageTwelveFile, "w") as message12writer:
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
                        message8writer.write(str(jsonObj) + "\n")
                    elif msgType == 12:
                        message12writer.write(str(jsonObj) + "\n")


if __name__ == '__main__':
    # use argparse here
    sourceFile = r"C:\Users\ketan\Downloads\pretrade_current.txt"
    targetMessage8File = r".\m8.json"
    targetMessage12File = r".\m12.json"
    targetTsvFile = r".\pretrade_current.tsv"

    # take the original source file and fix the json and split into 2 json files
    # one for each message type
    fixAndSplitInputFile(sourceFile, targetMessage8File, targetMessage12File)

    spark = SparkSession.builder \
        .appName('Aquis2') \
        .master("local[2]") \
        .getOrCreate()

    # collect messages of type 8, only keep the required fields
    messagesType8Df = spark.read.json(targetMessage8File) \
        .select("security_.securityId_", "security_.isin_", "security_.currency_")

    # messagesType8Df.printSchema()
    # root
    # | -- securityId_: long(nullable=true)
    # | -- isin_: string(nullable=true)
    # | -- currency_: string(nullable=true)

    messageType12Df = spark.read.json(targetMessage12File)

    # messageType12Df.printSchema()
    # root
    # | -- bookEntry_: struct(nullable=true)
    # | | -- orderId_: long(nullable=true)
    # | | -- price_: long(nullable=true)
    # | | -- quantity_: long(nullable=true)
    # | | -- securityId_: long(nullable=true)
    # | | -- side_: string(nullable=true)
    # | -- header: struct(nullable=true)
    # | | -- length_: long(nullable=true)
    # | | -- msgType_: long(nullable=true)
    # | | -- seqNo_: long(nullable=true)

    # now aggregate messageType12 by securityId_ and side_
    aggDfSells = messageType12Df.filter("bookEntry_.side_ == 'SELL'") \
        .select("*", (col("bookEntry_.quantity_") * col("bookEntry_.price_")).alias("TotalSellAmount"))\
        .groupby("bookEntry_.securityId_", "bookEntry_.side_") \
        .agg(count("bookEntry_.securityId_").alias("Total Sell Count"), \
             _sum("bookEntry_.quantity_").alias("Total Sell Quantity"), \
             _min("bookEntry_.price_").alias("Min Sell Price"), \
             _sum("TotalSellAmount").alias("Weighted Average Sell Price")\
             )\
        .withColumn("Weighted Average Sell Price", col("Weighted Average Sell Price")/col("Total Sell Quantity"))

    # aggDfSells.show()

    # now aggregate messageType12 by securityId_ and side_
    aggDfBuys = messageType12Df.filter("bookEntry_.side_ == 'BUY'") \
        .select("*", (col("bookEntry_.quantity_") * col("bookEntry_.price_")).alias("TotalBuyAmount"))\
        .groupby("bookEntry_.securityId_", "bookEntry_.side_") \
        .agg(count("bookEntry_.securityId_").alias("Total Buy Count"), \
             _sum("bookEntry_.quantity_").alias("Total Buy Quantity"), \
             _max("bookEntry_.price_").alias("Max Buy Price"), \
             _sum("TotalBuyAmount").alias("Weighted Average Buy Price")\
             )\
        .withColumn("Weighted Average Buy Price", col("Weighted Average Buy Price")/col("Total Buy Quantity"))

    # aggDfBuys.show()

    # bring it together with joins, use outer join with the security data due to missing ids
    # select columns in the following order..
    outputColList = [col("isin_").alias("ISIN"), col("currency_").alias("Currency"), "Total Buy Count", "Total Sell Count", "Total Buy Quantity", "Total Sell Quantity",
                "Weighted Average Buy Price", "Weighted Average Sell Price", "Max Buy Price", "Min Sell Price"]

    outputDf = aggDfBuys.join(aggDfSells, ["securityId_"])\
    .join(messagesType8Df, ["securityId_"], "left_outer")\
    .select(outputColList)

    print(outputDf.count())

    # collect into a single file
    outputDf.coalesce(1).write.option("sep", "\t").csv(r".\test.csv", header=True)

    spark.stop()
