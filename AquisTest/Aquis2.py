from pyspark.sql import SparkSession
from Aquis1 import filterIn, fixJson
from AquisCommon import timing_val
import json
from pyspark.sql.functions import sum as _sum, min as _min, max as _max
from pyspark.sql.functions import count, col, avg, concat, lit, expr, regexp_replace, from_json


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

@timing_val
def useSpark(sourceFile: str, targetMessage8File: str, targetMessage12File: str, targetTsvFile: str) -> None:
    # take the original source file and fix the json and split into 2 json files
    # one for each message type
    fixAndSplitInputFile(sourceFile, targetMessage8File, targetMessage12File)

    spark = SparkSession.builder \
        .appName('Aquis2') \
        .master("local[2]") \
        .getOrCreate()

    # clean data from source file
    cleanDf = spark.read.text(sourceFile) \
        .filter(col("value").contains("msgType_") & ~col("value").contains('msgType_":11')) \
        .withColumn("value", expr("substring(value,2)")) \
        .withColumn("value", regexp_replace("value", '\{\{', r'\{"header":\{')) \
        .withColumn("value", regexp_replace("value", 'SELL', '"SELL"')) \
        .withColumn("value", regexp_replace("value", 'BUY', '"BUY"')) \
        .withColumn("value", regexp_replace("value", '"flags_":"\{"', '"flags_":\{"'))

    # figure out schema on message 8
    msg8Schema = spark.read.json(cleanDf.filter(col("value").contains('"msgType_":8'))\
                                 .select(col("value").cast("string")).rdd.map(lambda r: r.value)).schema
    msg8Df = cleanDf.filter(col("value").contains('"msgType_":8')).withColumn("value",from_json("value", msg8Schema))
    # msg8Df.printSchema()
    # root
    # | -- value: struct(nullable=true)
    # | | -- header: struct(nullable=true)
    # | | | -- length_: long(nullable=true)
    # | | | -- msgType_: long(nullable=true)
    # | | | -- seqNo_: long(nullable=true)
    # | | -- security_: struct(nullable=true)
    # | | | -- currency_: string(nullable=true)
    # | | | -- flags_: struct(nullable=true)
    # | | | | -- b_: struct(nullable=true)
    # | | | | | -- aodEnabled_: long(nullable=true)
    # | | | | | -- closingEnabled_: long(nullable=true)
    # | | | | | -- illiquid: long(nullable=true)
    # | | | | | -- live_: long(nullable=true)
    # | | | | | -- testStock_: long(nullable=true)
    # | | | | -- v_: long(nullable=true)
    # | | | -- isin_: string(nullable=true)
    # | | | -- mic_: string(nullable=true)
    # | | | -- securityId_: long(nullable=true)
    # | | | -- tickTableId_: long(nullable=true)
    # | | | -- umtf_: string(nullable=true)

    # figure out schema on message 12
    msg12Schema = spark.read.json(cleanDf.filter(col("value").contains('"msgType_":12'))\
                                 .select(col("value").cast("string")).rdd.map(lambda r: r.value)).schema
    msg12Df = cleanDf.filter(col("value").contains('"msgType_":12')).withColumn("value",from_json("value", msg12Schema))
    # msg12Df.printSchema()
    # msg12Df.select("value.bookEntry_.side_").show()
    # root
    # | -- value: struct(nullable=true)
    # | | -- bookEntry_: struct(nullable=true)
    # | | | -- orderId_: long(nullable=true)
    # | | | -- price_: long(nullable=true)
    # | | | -- quantity_: long(nullable=true)
    # | | | -- securityId_: long(nullable=true)
    # | | | -- side_: string(nullable=true)
    # | | -- header: struct(nullable=true)
    # | | | -- length_: long(nullable=true)
    # | | | -- msgType_: long(nullable=true)
    # | | | -- seqNo_: long(nullable=true)

    # now aggregate messageType12 by securityId_ and side_
    aggDfSells = msg12Df.filter("value.bookEntry_.side_ == 'SELL'") \
        .select("*", (col("value.bookEntry_.quantity_") * col("value.bookEntry_.price_")).alias("TotalSellAmount")) \
        .groupby("value.bookEntry_.securityId_", "value.bookEntry_.side_") \
        .agg(count("value.bookEntry_.securityId_").alias("Total Sell Count"), \
             _sum("value.bookEntry_.quantity_").alias("Total Sell Quantity"), \
             _min("value.bookEntry_.price_").alias("Min Sell Price"), \
             _sum("TotalSellAmount").alias("Weighted Average Sell Price") \
             ) \
        .withColumn("Weighted Average Sell Price", col("Weighted Average Sell Price") / col("Total Sell Quantity"))


    # now aggregate messageType12 by securityId_ and side_
    aggDfBuys = msg12Df.filter("value.bookEntry_.side_ == 'BUY'") \
        .select("*", (col("value.bookEntry_.quantity_") * col("value.bookEntry_.price_")).alias("TotalBuyAmount")) \
        .groupby("value.bookEntry_.securityId_", "value.bookEntry_.side_") \
        .agg(count("value.bookEntry_.securityId_").alias("Total Buy Count"), \
             _sum("value.bookEntry_.quantity_").alias("Total Buy Quantity"), \
             _max("value.bookEntry_.price_").alias("Max Buy Price"), \
             _sum("TotalBuyAmount").alias("Weighted Average Buy Price") \
             ) \
        .withColumn("Weighted Average Buy Price", col("Weighted Average Buy Price") / col("Total Buy Quantity"))


    # bring it together with joins, use outer join with the security data due to missing ids
    # select columns in the following order..
    outputColList = [col("value.security_.isin_").alias("ISIN"), col("value.security_.currency_").alias("Currency"), "Total Buy Count",
                     "Total Sell Count", "Total Buy Quantity", "Total Sell Quantity",
                     "Weighted Average Buy Price", "Weighted Average Sell Price", "Max Buy Price", "Min Sell Price"]

    outputDf = aggDfBuys.join(aggDfSells, ["securityId_"], "full_outer")
    outputDf = outputDf.join(msg8Df, col("securityId_") == col("value.security_.securityId_") , "left_outer") \
        .na.fill(0, outputColList[2:]) \
        .na.fill("MISSING", ["value.security_.isin_", "value.security_.currency_"]) \
        .select(outputColList)

    # collect into a single file
    outputDf.coalesce(1).write.option("sep", "\t").csv(r".\test.csv", header=True)

    spark.stop()


if __name__ == '__main__':
    # use argparse here
    sourceFile = r"C:\Users\ketan\Downloads\pretrade_current.txt"
    targetMessage8File = r".\m8.json"
    targetMessage12File = r".\m12.json"
    targetTsvFile = r".\pretrade_current.tsv"
    timer, _, _ = useSpark(sourceFile, targetMessage8File, targetMessage12File, targetTsvFile)
    print("Time:", timer)
