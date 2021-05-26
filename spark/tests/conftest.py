import pytest
from typing import Any
from typing import Optional
import src.SparkDFCompare as dfc
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame


@pytest.fixture(scope="module")
@pytest.mark.skip("Skip test")
def annotations_collect():
    """Fixture to collect 'annotations' for pyannonate

    Yields:
        [None]: [Generates file annotations.txt]
    """
    # pipx install pyannotate==1.2.0
    from pyannotate_runtime import collect_types

    collect_types.init_types_collection()
    collect_types.start()
    yield None
    collect_types.stop()
    collect_types.dump_stats("annotations.txt")


@pytest.mark.skip("Skip test")
def test_pyannotation_collect(annotations_collect: Optional[Any]) -> None:
    """[Trigger to build fixture annotations_collect]

    Args:
        annotations_collect (Optional[Any]): [Fixture name]
    """
    pass


@pytest.fixture
def sparkConf():
    """Generate sparkConfig fixture

    Returns:
        [SparkConfig]: [A spark config object]
    """
    import os
    from dotenv import load_dotenv
    load_dotenv(verbose=True)
    return dfc.setSparkConfig(jars=os.getenv("JARS"))


@pytest.fixture
def spark(sparkConf) -> SparkSession:
    """Generate spark session fixture given a spark config fixture

    Args:
        sparkConf ([SparConfig]): [A spark config fixture]

    Returns:
        SparkSession: [A spark session fixture]
    """
    return dfc.getSpark(sparkConf)


@pytest.fixture
def df1(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with two columns.
       (letters, numbers).
       (letters,numbers)=[("a",10),("b,20),("c",30)]

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]
    """
    dict_lst = {"letters": ["a", "b", "c"], "numbers": [10, 20, 30]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df2(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with two columns.
       (letters, numbers).
       (letters,numbers)=[("a",1),("b,2),("c",3)]

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]
    """
    dict_lst = {"letters": ["a", "b", "c"], "numbers": [1, 2, 3]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df3(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with two columns.
       (letters, numbers).
       (letters,numbers)=[("a",10),("b,20),("c",30),("d",40)]

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]
    """

    dict_lst = {"letters": ["a", "b", "c", "d"], "numbers": [10, 20, 30, 40]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df4(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with two columns.
       (letters, numbers).
       (letters,numbers)=[("z",1),("y,2),("x",3)]

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]
    """

    dict_lst = {"letters": ["z", "y", "x"], "numbers": [1, 2, 3]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df5(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with two columns. With Null in the columns
       (letters, numbers).
       (letters,numbers)=[("z",1),("None,2),("x",None)]

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]
    """

    dict_lst = {"letters": ["z", None, "x"], "numbers": [1, 2, None]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df6(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with two columns. With BLANK valued columns
       (letters, numbers).
       (letters,numbers)=[("a",1),("",2),("b",3),(" ",4)]

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]
    """

    dict_lst = {"letters": ["a", "", "b", " "], "numbers": [1, 2, 3, 4]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def df7(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with two columns. With BLANK valued columns
       (letters, numbers).
       (letters,numbers)=[("a",1),("o1",2),("b",3),("o2",4)]

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]
    """
    dict_lst = {"letters": ["a", "o1", "b", "o2"], "numbers": [1, 2, 3, 4]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def dbUrl():
    """Fixture to generate a postgresql URL

    Returns:
        [str]: [Fixture representing a postgresql URL]
    """
    from dotenv import load_dotenv
    import os
    load_dotenv(verbose=True)
    return dfc.getUrl(db=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"), secret=os.getenv("POSTGRES_SECRET"))


@pytest.fixture
def df_from_db_left(spark: SparkSession, dbUrl) -> DataFrame:
    """Fixture to generate a spark dataframe by reading a DB table

    Args:
        spark (SparkSession): [Spark session]
        dbUrl ([str]): [postgresql URL]

    Returns:
        DataFrame: [Spark dataframe built using a SQL SELECT]
    """
    sql = "SELECT * FROM tleft"
    return dfc.getQueryDataFrame(spark, dbUrl, sql)


@pytest.fixture
def df_from_db_right(spark: SparkSession, dbUrl) -> DataFrame:
    """Fixture to generate a spark dataframe by reading a DB table

    Args:
        spark (SparkSession): [Spark session]
        dbUrl ([str]): [postgresql URL]

    Returns:
        DataFrame: [Spark dataframe built using a SQL SELECT]
    """

    sql = "SELECT * FROM tright"
    return dfc.getQueryDataFrame(spark, dbUrl, sql)
