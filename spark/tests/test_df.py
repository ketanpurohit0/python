import src.SparkDFCompare as dfc
import tests.common as c
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame


def test_all_same(spark: SparkSession, df1: DataFrame) -> None:
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
    totalNulls = c.countNullsAcrossAllColumns(df5)
    assert totalNulls > 0

    # now replace NULLS
    df5 = dfc.replaceNulls(df5)
    totalNulls = c.countNullsAcrossAllColumns(df5)
    assert totalNulls == 0


def test_blank_replacement(spark: SparkSession, df6: DataFrame) -> None:
    totalBlanks = c.countWSAcrossAllStringColumns(df6)
    assert totalBlanks == 2

    # now replace BLANKS
    df6 = dfc.replaceBlanks(df6)
    totalBlanks = c.countWSAcrossAllStringColumns(df6)
    assert totalBlanks == 0
