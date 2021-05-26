import src.SparkDFCompare as dfc
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame


def test_from_db_self(spark: SparkSession, df_from_db_left: DataFrame) -> None:
    """[Given a spark dataframe - to a side-by-side compare of it with itself. Expect no differences.
        dataframe is sourced from database.]

    Args:
        spark (SparkSession): [Spark session]
        df_from_db_left (DataFrame): [A spark dataframe sourced from database]
    """
    dfResult = dfc.compareDfs(
        spark,
        df_from_db_left,
        df_from_db_left,
        tolerance=0.1,
        keysLeft="bsr",
        keysRight="bsr",
        colExcludeList=["n1", "n2", "n3", "n4", "n5", "tx"],
        joinType="full_outer",
    )
    pass_count = dfResult.filter("PASS == True").count()
    overall_count = dfResult.count()
    assert pass_count == overall_count


def test_from_db(spark: SparkSession, df_from_db_left: DataFrame, df_from_db_right: DataFrame) -> None:
    """[Compare two dataframe sourced from database. One side has NULLS/BLANKs in database. Expect no differences]

    Args:
        spark (SparkSession): [Spark session]
        df_from_db_left (DataFrame): [Spark dataframe source from database.]
        df_from_db_right (DataFrame): [Spark dataframe source from database]
    """
    dfResult = dfc.compareDfs(
        spark,
        df_from_db_left,
        df_from_db_right,
        tolerance=0.1,
        keysLeft="bsr",
        keysRight="bsr",
        colExcludeList=["n1", "n2", "n3", "n4", "n5", "tx"],
        joinType="full_outer",
    )
    pass_count = dfResult.filter("PASS == True").count()
    overall_count = dfResult.count()
    assert pass_count == overall_count


def test_from_db_column(spark: SparkSession, df_from_db_left: DataFrame, df_from_db_right: DataFrame) -> None:
    """[Compare two dataframe sourced from database. One side has NULLS/BLANKs in database. Expect no differences
    One side has a column not on right side and vice versa]

    Args:
        spark (SparkSession): [Spark session]
        df_from_db_left (DataFrame): [Spark dataframe source from database.]
        df_from_db_right (DataFrame): [Spark dataframe source from database]
    """
    from pyspark.sql.functions import lit
    df_from_db_left_mut = df_from_db_left.withColumn("LEFT_COL", lit("left_col_value"))
    df_from_db_right_mut = df_from_db_right.withColumn("RIGHT_COL", lit("right_col_value"))
    dfResult = dfc.compareDfs(
        spark,
        df_from_db_left_mut,
        df_from_db_right_mut,
        tolerance=0.1,
        keysLeft="bsr",
        keysRight="bsr",
        colExcludeList=["n1", "n2", "n3", "n4", "n5", "tx", "this_col_does_not_exist"],
        joinType="full_outer",
    )
    pass_count = dfResult.filter("PASS == True").count()
    overall_count = dfResult.count()
    assert pass_count == overall_count
