import pytest
import SparkHelper as sh
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Any
from typing import Optional


@pytest.fixture(scope="module")
@pytest.mark.skip("Skip test")
def annotations_collect():
    # pipx install pyannotate==1.2.0
    from pyannotate_runtime import collect_types

    collect_types.init_types_collection()
    collect_types.start()
    yield None
    collect_types.stop()
    collect_types.dump_stats("annotations.txt")


@pytest.mark.skip("Skip test")
def test_pyannotation_collect(annotations_collect: Optional[Any]) -> None:
    pass


@pytest.fixture
def sparkConf():
    import os
    from dotenv import load_dotenv
    load_dotenv(verbose=True)
    return sh.setSparkConfig(jars=os.getenv("JARS"))


@pytest.fixture
def spark(sparkConf) -> SparkSession:
    return sh.getSpark(sparkConf)


@pytest.fixture
def df1(spark: SparkSession) -> DataFrame:
    dict_lst = {"letters": ["a", "b", "c"], "numbers": [10, 20, 30]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df2(spark: SparkSession) -> DataFrame:
    dict_lst = {"letters": ["a", "b", "c"], "numbers": [1, 2, 3]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df3(spark: SparkSession) -> DataFrame:
    dict_lst = {"letters": ["a", "b", "c", "d"], "numbers": [10, 20, 30, 40]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df4(spark: SparkSession) -> DataFrame:
    dict_lst = {"letters": ["z", "y", "x"], "numbers": [1, 2, 3, 4]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df5(spark: SparkSession) -> DataFrame:
    dict_lst = {"letters": ["z", None, "x"], "numbers": [1, 2, None, 4]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df6(spark: SparkSession) -> DataFrame:
    dict_lst = {"letters": ["a", "", "b", " "], "numbers": [1, 2, 3, 4]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


def test_all_same(spark: SparkSession, df1: DataFrame) -> None:
    dfResult = sh.compareDfs(
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
    dfResult = sh.compareDfs(
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
    dfResult = sh.compareDfs(
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
    dfResult = sh.compareDfs(
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


def countNullsAcrossAllColumns(df: DataFrame) -> int:
    # https://www.datasciencemadesimple.com/count-of-missing-nanna-and-null-values-in-pyspark/
    from pyspark.sql.functions import isnull, when, count, expr

    nullCountDf = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
    sumExpr = "+".join(nullCountDf.columns) + " as TOTAL"
    sumDf = nullCountDf.select(expr(sumExpr))
    return sumDf.collect()[0].TOTAL


def countWSAcrossAllStringColumns(df: DataFrame) -> int:
    from pyspark.sql.functions import col, when, count, trim, expr

    stringCols = [cn for (cn, ct) in df.dtypes if ct == "string"]
    blanksCountdf = df.select(
        [count(when(trim(col(c)) == "", True)).alias(c) for c in stringCols]
    )
    sumExpr = "+".join(blanksCountdf.columns) + " as TOTAL"
    sumDf = blanksCountdf.select(expr(sumExpr))
    return sumDf.collect()[0].TOTAL


def test_null_replacement(spark: SparkSession, df5: DataFrame) -> None:
    totalNulls = countNullsAcrossAllColumns(df5)
    assert totalNulls > 0

    # now replace NULLS
    df5 = sh.replaceNulls(df5)
    totalNulls = countNullsAcrossAllColumns(df5)
    assert totalNulls == 0


def test_blank_replacement(spark: SparkSession, df6: DataFrame) -> None:
    totalBlanks = countWSAcrossAllStringColumns(df6)
    assert totalBlanks == 2

    # now replace BLANKS
    df6 = sh.replaceBlanks(df6)
    totalBlanks = countWSAcrossAllStringColumns(df6)
    assert totalBlanks == 0


