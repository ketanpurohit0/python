import src.SparkDFCompare as dfc
import tests.common
import tests.common as c
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from collections import namedtuple
from pyspark.sql.functions import when, expr
from datetime import datetime


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


def test_orphans_diffent_keys(spark: SparkSession, df6: DataFrame, df8: DataFrame) -> None:
    """[Compare dataframes where there are expected to be matches and orphans, use different key names on each side]

    Args:
        spark (SparkSession): [Spark session]
        df6 (DataFrame): [A spark dataframe with BLANKS in the values]
        df8 (DataFrame): [A spark dataframe]
    """
    dfResult = dfc.compareDfs(
        spark,
        df6,
        df8,
        tolerance=0.1,
        keysLeft="letters",
        keysRight="letters2",
        colExcludeList=[],
        joinType="full_outer",
    )
    pass_count = dfResult.filter("PASS == True").count()
    assert pass_count == 2
    orphans_count = dfResult.filter("PASS IS NULL").count()
    assert orphans_count == 4
    orphans_count_left = dfResult.filter("PASS IS NULL AND letters_left IS NULL").count()
    orphans_count_right = dfResult.filter("PASS IS NULL AND letters2_right IS NULL").count()
    assert orphans_count_left == 2 and orphans_count_right == 2


def test_group_status(spark: SparkSession, df_group_status: DataFrame) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType

    df_group_status.show()
    df_group_status.printSchema()

    df_enrich: DataFrame = df_group_status \
                    .withColumn("cond1", when(col("dt") >= to_date(lit('2020-01-01'), 'yyyy-MM-dd'), lit(True)).otherwise(lit(False))) \
                    .withColumn("cond2", when(col("dt") >= to_date(lit('2021-01-01'), 'yyyy-MM-dd'), lit(True)).otherwise(lit(False)))

    df_enrich.show()
    df_enrich.printSchema()

    df_enrich_further: DataFrame = df_enrich.groupBy("grp") \
                        .agg(F.collect_set("cond1"), F.collect_set("cond2")).toDF(*["grp", "cond1_set", "cond2_set"])

    df_enrich_further.show()
    df_enrich_further.printSchema()

    df_final: DataFrame = df_enrich_further.withColumn("from_cond1_set", ~F.array_contains(F.col("cond1_set"), False)) \
                        .withColumn("from_cond2_set", ~F.array_contains(F.col("cond2_set"), False))

    df_final.show()
    df_final.printSchema()

    df_final: DataFrame = df_final.drop(*["cond1_set", "cond2_set"])
    df_enrich: DataFrame = df_enrich.drop(*["cond1", "cond2"])

    df_enrich.join(df_final, df_enrich["grp"] == df_final["grp"], "inner").show()


def test_adjustment(spark: SparkSession, dfAdj: DataFrame, modifications_list: list):

    tests.common.printSparkConf(spark)

    flag_col = "isModified"
    partition_cols = ["dept_name"]
    check_point_interval = 5
    dfAdj = dfAdj.withColumn(flag_col, lit(False)).repartition(4, *partition_cols).cache()

    print(f"data_count:{dfAdj.count()}, partition_count:{dfAdj.rdd.getNumPartitions()}, rule_count: {len(modifications_list)}")
    # root
    # |-- dept_name: string (nullable = true)
    # |-- dept_id: long (nullable = true)

    # col, value, where
    check_point_stale = True
    Modification = namedtuple('Modification', 'col set where')
    for index, m in enumerate(modifications_list, 1):
        mod = Modification(*m)
        # print("start", mod, datetime.now())
        dfAdj = dfAdj.withColumn(flag_col, when(expr(mod.where), lit(True)).otherwise(col(flag_col)))\
                     .withColumn(mod.col, when(expr(mod.where), lit(mod.set)).otherwise(col(mod.col)))
        check_point_stale = True
        if index % check_point_interval == 0:
            dfAdj = dfAdj.cache().checkpoint(True)
            check_point_stale = False
        # print("end", mod, datetime.now())

    if check_point_stale:
        dfAdj = dfAdj.checkpoint(True)

    dfAdj = dfAdj.filter(f"{flag_col} = True").drop(flag_col).cache()

    # force action, takes a looong time if there are a large number of transforms, even with a small df
    rows_affected = dfAdj.count()
    print(f"Rows affected: {rows_affected}")
    print("completed", datetime.now())


