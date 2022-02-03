from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder \
                        .appName('SparkByExamples.com') \
                        .master("local[2]") \
                        .getOrCreate()


    data = [('James','Smith','M',3000),
      ('Anna','Rose','F',4100),
      ('Robert','Williams','M',6200),
    ]

    columns = ["firstname","lastname","gender","salary"]
    df = spark.createDataFrame(data=data, schema = columns)
    df.show()