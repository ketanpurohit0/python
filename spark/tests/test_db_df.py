import SparkHelper as sh
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame


def test_from_db_self(spark: SparkSession, df_from_db_left: DataFrame) -> None:
    dfResult = sh.compareDfs(
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
    dfResult = sh.compareDfs(
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
