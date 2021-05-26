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
    orphan_count = dfResult.filter("PASS IS NULL").count()
    assert dfResult.count() == orphan_count


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


def test_orphans(spark: SparkSession, df6: DataFrame, df7: DataFrame) -> None:
    """[Compare dataframes where there are expected to be matches and orphans]

    Args:
        spark (SparkSession): [Spark session]
        df6 (DataFrame): [A spark dataframe with BLANKS in the values]
        df7 (DataFrame): [A spark dataframe]
    """
    dfResult = dfc.compareDfs(
        spark,
        df6,
        df7,
        tolerance=0.1,
        keysLeft="letters",
        keysRight="letters",
        colExcludeList=[],
        joinType="full_outer",
    )
    pass_count = dfResult.filter("PASS == True").count()
    assert pass_count == 2
    orphans_count = dfResult.filter("PASS IS NULL").count()
    assert orphans_count == 4
    orphans_count_left = dfResult.filter("PASS IS NULL AND letters_left IS NULL").count()
    orphans_count_right = dfResult.filter("PASS IS NULL AND letters_right IS NULL").count()
    assert orphans_count_left == 2 and orphans_count_right == 2
