import src.SparkDFCompare as dfc
import tests.common as c
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame


def test_all_same(spark: SparkSession, df1: DataFrame) -> None:
    """[Compare a dataframe against itself. Expect no differences]

    Args:
        spark (SparkSession): [Spark session]
        df1 (DataFrame): [A spark dataframe]
    """
    dfResult = dfc.compareDfs(
        spark,
        df1,
        df1,
        tolerance=0.1,
        keysLeft="letters",
        keysRight="letters",
        colExcludeList=[],
        joinType="full_outer",
    )
    pass_count = dfResult.filter("PASS == True").count()
    overall_count = dfResult.count()
    assert pass_count == overall_count
    assert df1.count() == overall_count


def test_all_different(spark: SparkSession, df1: DataFrame, df2: DataFrame) -> None:
    """[Compare spark dataframes. Expect all rows to be different]

    Args:
        spark (SparkSession): [Spark session]
        df1 (DataFrame): [A spark dataframe]
        df2 (DataFrame): [A spark dataframe]
    """
    dfResult = dfc.compareDfs(
        spark,
        df1,
        df2,
        tolerance=0.1,
        keysLeft="letters",
        keysRight="letters",
        colExcludeList=[],
        joinType="full_outer",
    )
    pass_count = dfResult.filter("PASS == True").count()
    assert pass_count == 0


def test_partial_same(spark: SparkSession, df1: DataFrame, df3: DataFrame) -> None:
    """Compare spark dataframes. Expect partial matches

    Args:
        spark (SparkSession): [Spark session]
        df1 (DataFrame): [A spark dataframe]
        df3 (DataFrame): [A spark dataframe]
    """
    dfResult = dfc.compareDfs(
        spark,
        df1,
        df3,
        tolerance=0.1,
        keysLeft="letters",
        keysRight="letters",
        colExcludeList=[],
        joinType="full_outer",
    )
    pass_count = dfResult.filter("PASS == True").count()
    overall_count = dfResult.count()
    assert pass_count < overall_count
    assert pass_count > 0


def test_no_common(spark: SparkSession, df1: DataFrame, df4: DataFrame) -> None:
    """[Compare spark dataframes. Expect all to be different]

    Args:
        spark (SparkSession): [Spark session]
        df1 (DataFrame): [A spark dataframe]
        df4 (DataFrame): [A spark dataframe]
    """
    dfResult = dfc.compareDfs(
        spark,
        df1,
        df4,
        tolerance=0.1,
        keysLeft="letters",
        keysRight="letters",
        colExcludeList=[],
        joinType="full_outer",
    )
    pass_count = dfResult.filter("PASS == True").count()
    assert pass_count == 0


def test_null_replacement(spark: SparkSession, df5: DataFrame) -> None:
    """[Check for NULL replacement in dataframe. Expect there to be NULLS in dataframe before
    and none after]

    Args:
        spark (SparkSession): [Spark session]
        df5 (DataFrame): [A spark dataframe]
    """
    totalNulls = c.countNullsAcrossAllColumns(df5)
    assert totalNulls > 0

    # now replace NULLS
    df5 = dfc.replaceNulls(df5)
    totalNulls = c.countNullsAcrossAllColumns(df5)
    assert totalNulls == 0


def test_blank_replacement(spark: SparkSession, df6: DataFrame) -> None:
    """[Check for BLANK replacement in dataframe. Expect there to be BLANKS in dataframe before
    and none after. BLANKs include a zero length string]

    Args:
        spark (SparkSession): [Spark session]
        df6 (DataFrame): [A spark dataframe with BLANKS in the values]
    """

    totalBlanks = c.countWSAcrossAllStringColumns(df6)
    assert totalBlanks == 2

    # now replace BLANKS
    df6 = dfc.replaceBlanks(df6)
    totalBlanks = c.countWSAcrossAllStringColumns(df6)
    assert totalBlanks == 0


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
    json = '{"ConstantRiser" : {"2 days" : 12, "3 days" : 15, "4 days" : 18, "5 days" : 67.1}}'
    df = spark.read.json(spark.sparkContext.parallelize([json]))
    assert(df.count() == 1)
    assert(df.filter("ConstantRiser['3 days']> 0").count(), 1)
