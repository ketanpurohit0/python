from pyspark.sql import SparkSession
from AquisCommon import timing_val
from pyspark.sql.functions import sum as _sum, min as _min, max as _max
from pyspark.sql.functions import count, col, expr, regexp_replace, from_json
import os
from dotenv import load_dotenv
from pyspark import SparkConf


def getDbConnectionUrl(db, user, secret):
    """[Connection URL for 'jdbc' and postgresql database]

    Args:
        db ([str]): [Database name]
        user ([str]): [User Id]
        secret ([str]): [Password]

    Returns:
        [str]: [jdbc url for postgresql]
    """
    return f"jdbc:postgresql://localhost/{db}?user={user}&password={secret}"


def getJars():
    return os.getenv("JARS")


def getSparkConf(jars: str):
    print(jars)
    conf = SparkConf()
    if jars is not None:
        conf.set("spark.jars", jars)
    return conf


@timing_val
def useSpark(sourceFile: str, targetTsvFile: str) -> None:
    load_dotenv(verbose=True)

    spark = SparkSession.builder \
        .appName('Aquis2') \
        .master("local[2]") \
        .config(conf=getSparkConf(getJars())) \
        .getOrCreate()

    # clean data from source file
    cleanDf = spark.read.text(sourceFile) \
        .filter(col("value").contains("msgType_") & ~col("value").contains('msgType_":11')) \
        .withColumn("value", expr("substring(value,2)")) \
        .withColumn("value", regexp_replace("value", '\{\{', r'\{"header":\{')) \
        .withColumn("value", regexp_replace("value", 'SELL', '"SELL"')) \
        .withColumn("value", regexp_replace("value", 'BUY', '"BUY"')) \
        .withColumn("value", regexp_replace("value", '"flags_":"\{"', '"flags_":\{"'))

    # figure out schema on message 8, keep for re-use later as a technology demonstration
    msg8Schema = spark.read.json(cleanDf.filter(col("value").contains('"msgType_":8'))
                                 .select(col("value").cast("string")).rdd.map(lambda r: r.value))._jdf.schema().toDDL()
    msg8Df = cleanDf.filter(col("value").contains('"msgType_":8')).withColumn("value", from_json("value", msg8Schema)) \
        .select("value.security_.securityId_", "value.security_.isin_", "value.security_.currency_") \
        .repartition(2, ["securityId_"])
    # msg8Df.printSchema()
    # root
    # | -- securityId_: long(nullable=true)
    # | -- isin_: string(nullable=true)
    # | -- currency_: string(nullable=true)

    # figure out schema on message 12, keep for re-use later as a technology demonstration
    msg12Schema = spark.read.json(cleanDf.filter(col("value").contains('"msgType_":12'))
                                  .select(col("value").cast("string")).rdd.map(lambda r: r.value))._jdf.schema().toDDL()
    msg12Df = cleanDf.filter(col("value").contains('"msgType_":12')) \
        .withColumn("value", from_json("value", msg12Schema)) \
        .repartition(2, ["value.bookEntry_.securityId_"])

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
        .groupby("value.bookEntry_.securityId_") \
        .agg(count("value.bookEntry_.securityId_").alias("Total Sell Count"),
             _sum("value.bookEntry_.quantity_").alias("Total Sell Quantity"),
             _min("value.bookEntry_.price_").alias("Min Sell Price"),
             _sum("TotalSellAmount").alias("Weighted Average Sell Price")
             ) \
        .withColumn("Weighted Average Sell Price", col("Weighted Average Sell Price") / col("Total Sell Quantity"))

    # now aggregate messageType12 by securityId_ and side_
    aggDfBuys = msg12Df.filter("value.bookEntry_.side_ == 'BUY'") \
        .select("*", (col("value.bookEntry_.quantity_") * col("value.bookEntry_.price_")).alias("TotalBuyAmount")) \
        .groupby("value.bookEntry_.securityId_") \
        .agg(count("value.bookEntry_.securityId_").alias("Total Buy Count"),
             _sum("value.bookEntry_.quantity_").alias("Total Buy Quantity"),
             _max("value.bookEntry_.price_").alias("Max Buy Price"),
             _sum("TotalBuyAmount").alias("Weighted Average Buy Price")) \
        .withColumn("Weighted Average Buy Price", col("Weighted Average Buy Price") / col("Total Buy Quantity"))

    # bring it together with joins, use outer join with the security data due to missing ids
    # select columns in the following order..
    outputColList = [col("isin_").alias("ISIN"), col("currency_").alias("Currency"), "Total Buy Count",
                     "Total Sell Count", "Total Buy Quantity", "Total Sell Quantity",
                     "Weighted Average Buy Price", "Weighted Average Sell Price", "Max Buy Price", "Min Sell Price"]

    outputDf = aggDfBuys.join(aggDfSells, ["securityId_"], "full_outer") \
        .join(msg8Df, ["securityId_"], "left_outer") \
        .na.fill(0, outputColList[2:]) \
        .na.fill("MISSING", ["isin_", "currency_"]) \
        .select(outputColList)

    # collect into a single file
    outputDf.coalesce(1).write.option("sep", "\t").csv(targetTsvFile, header=True)

    # Demo writing to postgresql (msg8 dataframe)
    dburl = getDbConnectionUrl(db=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"),
                               secret=os.getenv("POSTGRES_SECRET"))
    msg8Df.write.format("jdbc") \
        .option("url", dburl) \
        .option("dbtable", "AcquisExample") \
        .option("driver", "org.postgresql.Driver") \
        .save(mode="append")

    spark.stop()


if __name__ == '__main__':
    # use argparse here
    sourceFile = r"C:\Users\ketan\Downloads\pretrade_current.txt"
    targetTsvFile = r".\useSpark.tsv"
    timer, _, _ = useSpark(sourceFile, targetTsvFile)
    print("Time:", timer)
