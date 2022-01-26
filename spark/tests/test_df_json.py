from pyspark.sql.session import SparkSession


def test_json(spark: SparkSession) -> None:
    """
    root
    |-- Monthly: double (nullable = true)
    |-- Weekly: double (nullable = true)
    |-- Yearly: double (nullable = true)
    +-------+------+------+
    |Monthly|Weekly|Yearly|
    +-------+------+------+
    |   97.3|  97.3|  97.3|
    +-------+------+------+

    Args:
        spark (SparkSession): [Spark session]
    """
    json = '{"Weekly" : 97.3, "Monthly": 97.3, "Yearly": 97.3 }'
    df = spark.read.json(spark.sparkContext.parallelize([json]))
    assert (df.count() == 1)


def test_json_in_column(spark: SparkSession) -> None:
    """
    root
    |-- Monthly: double (nullable = true)
    |-- Weekly: struct (nullable = true)
    |    |-- A: string (nullable = true)
    |    |-- B: string (nullable = true)
    |    |-- C: double (nullable = true)
    |    |-- D: long (nullable = true)
    |-- Yearly: double (nullable = true)
    +-------+---------------+------+
    |Monthly|         Weekly|Yearly|
    +-------+---------------+------+
    |   97.3|[a, b, 3.14, 5]|  97.3|
    +-------+---------------+------+


    Args:
        spark (SparkSession): [Spark session]
    """
    json = '{"Weekly" : {"A" : "a", "B" : "b", "C" : 3.14, "D" : 5}, "Monthly": 97.3, "Yearly": 97.3 }'
    df = spark.read.json(spark.sparkContext.parallelize([json]))
    assert(df.count() == 1)
    assert(df.filter("Weekly.A == 'a'").count() == 1)
    assert(df.filter("Weekly.A == 'z'").count() == 0)


def test_json_highs(spark: SparkSession) -> None:
    """
    root
    |-- Highs: struct (nullable = true)
    |    |-- Monthly: double (nullable = true)
    |    |-- Weekly: double (nullable = true)
    |    |-- Yearly: double (nullable = true)
    +------------------+
    |             Highs|
    +------------------+
    |[97.3, 97.3, 97.3]|
    +------------------+

    Args:
        spark (SparkSession): [Spark session]
    """
    json = '{"Highs" : {"Weekly" : 97.3, "Monthly": 97.3, "Yearly": 97.3 }}'
    df = spark.read.json(spark.sparkContext.parallelize([json]))
    assert(df.count() == 1)
    assert(df.filter("Highs.Weekly > 0").count(), 1)
    assert(df.filter("Highs.Weekly > Highs.Monthly").count() == 0)


def test_json_risers(spark: SparkSession) -> None:
    """
    root
    |-- ConstantRiser: struct (nullable = true)
    |    |-- 2 days: long (nullable = true)
    |    |-- 3 days: long (nullable = true)
    |    |-- 4 days: long (nullable = true)
    |    |-- 5 days: double (nullable = true)
    +------------------+
    |     ConstantRiser|
    +------------------+
    |[12, 15, 18, 67.1]|
    +------------------+


    Args:
        spark (SparkSession): [Spark session]
    """
    json = '{"ConstantRiser" : {"2 days" : 12, "3 days" : -15, "4 days" : 18, "5 days" : 67.1}}'
    df = spark.read.json(spark.sparkContext.parallelize([json]))
    assert(df.count() == 1)
    assert(df.filter("ConstantRiser['3 days']> 0").count(), 1)
