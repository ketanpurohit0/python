import pytest
from typing import Any
from typing import Optional
import SparkDFCompare as dfc
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame


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
    return dfc.setSparkConfig(jars=os.getenv("JARS"))


@pytest.fixture
def spark(sparkConf) -> SparkSession:
    return dfc.getSpark(sparkConf)


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


@pytest.fixture
def dbUrl():
    from dotenv import load_dotenv
    import os
    load_dotenv(verbose=True)
    return dfc.getUrl(db=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"), secret=os.getenv("POSTGRES_SECRET"))


@pytest.fixture
def df_from_db_left(spark: SparkSession, dbUrl) -> DataFrame:
    sql = "SELECT * FROM tleft"
    return dfc.getQueryDataFrame(spark, dbUrl, sql)


@pytest.fixture
def df_from_db_right(spark: SparkSession, dbUrl) -> DataFrame:
    sql = "SELECT * FROM tright"
    return dfc.getQueryDataFrame(spark, dbUrl, sql)


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
