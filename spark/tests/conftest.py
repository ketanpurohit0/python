import pytest
from typing import Any
from typing import Optional

from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType

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
    conf = dfc.setSparkConfig(jars=os.getenv("JARS"))
    conf.set("spark.sql.shuffle.partitions", os.getenv("PARTITIONS"))
    conf.set("spark.master", os.getenv("SPARK_MASTER"))
    conf.set("spark.executor.instances", os.getenv("SPARK_EXECUTORS"))
    conf.set("spark.executor.cores", os.getenv("SPARK_CORES_PER_EXECUTOR"))
    return conf


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
def df8(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with two columns. With BLANK valued columns
       (letters2, numbers).
       (letters2,numbers)=[("a",1),("o1",2),("b",3),("o2",4)]

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]
    """
    dict_lst = {"letters2": ["a", "o1", "b", "o2"], "numbers": [1, 2, 3, 4]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def dfAdj(spark: SparkSession) -> DataFrame:

    data = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40), ("CTS", 41), ("CTS", 42)]
    for _ in range(3):
        data.extend(data)
    deptColumns = ["dept_name", "dept_id"]
    return spark.createDataFrame(data=data, schema=deptColumns)


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


@pytest.fixture
def df_group_status(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with three columns. With BLANK valued columns
       (grp, dt, status).

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]

        +---+----------+------+
        |grp|        dt|status|
        +---+----------+------+
        |  A|2020-03-21|  null|
        |  A|      null|  null|
        |  A|2020-03-22|  null|
        |  A|2020-03-25|  null|
        |  B|2020-02-21|  null|
        |  B|2020-02-22|  null|
        |  B|2020-02-25|  null|
        +---+----------+------+
    """
    from datetime import datetime
    dict_lst = {"grp": ["A", "A", "A", "A"], "dt": [datetime(2020, 3, 21), None, datetime(2020, 3, 22), datetime(2020, 3, 25)],
                "status": [None, None, None, None]}

    dict_lst2 = {"grp": ["B", "B", "B"], "dt": [datetime(2020, 2, 21), datetime(2020, 2, 22), datetime(2020, 2, 25)],
                "status": [None, None, None]}

    column_names, data = zip(*dict_lst.items())

    schema = StructType([
        StructField("grp", StringType(), True),
        StructField("dt", DateType(), True),
        StructField("status", BooleanType(), True)
        ])

    df1 = spark.createDataFrame(zip(*data), schema)

    column_names, data = zip(*dict_lst2.items())
    return df1.union(spark.createDataFrame(zip(*data), schema))


@pytest.fixture
def df_booleans(spark: SparkSession) -> DataFrame:
    """Generate a test spark dataframe with two columns. With BLANK valued columns
       (pass, bool1, bool2, bool3).
       (pass, bool1, bool2, bool3)=[(true, false, false, false),(true, false, true, true),(true, true, true, true),(true, false, false, true)]

    Args:
        spark (SparkSession): [Spark session fixture]

    Returns:
        DataFrame: [Test spark dataframe]
    """
    dict_lst = {"pass": [True, True, True, True],
                "bool1": [False, True, True, True],
                "bool2": [False, False, True, False],
                "bool3": [False, True, True, True]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


@pytest.fixture
def modifications_list() -> list:
    rules = [
            ("dept_name", "Marketing2.0", "dept_name = 'Marketing'"),
            ("dept_id", 30, "dept_name = 'CTS' AND dept_id = 42"),
            ("dept_id", 30, "dept_name = 'CTS' AND dept_id = 142"),
            ("dept_name", "Marketing2.0", "dept_name = 'XMarketing'"),
            ("dept_id", 30, "dept_name = 'XCTS' AND dept_id = 142"),
            ("dept_name", "Marketing2.0", "dept_name = 'XMarketing'"),
            ("dept_id", 30, "dept_name = 'xCTS' AND dept_id = 42"),
            ("dept_id", 30, "dept_name = 'xCTS' AND dept_id = 142"),
            ("dept_name", "xMarketing2.0", "dept_name = 'XMarketing'"),
        ("dept_name", "Marketing2.0", "dept_name = 'Marketing'"),
        ("dept_id", 30, "dept_name = 'CTS' AND dept_id = 42"),
        ("dept_id", 30, "dept_name = 'CTS' AND dept_id = 142"),
        ("dept_name", "Marketing2.0", "dept_name = 'XMarketing'"),
        ("dept_name", "cMarketing2.0", "dept_name = 'cXMarketing'"),

        ("dept_id", 30, "dept_name = 'CTS' AND dept_id = 142"),
        ("dept_name", "Marketing2.0", "dept_name = 'XMarketing'"),
        ("dept_name", "cMarketing2.0", "dept_name = 'cXMarketing'"),
    ]

    for _ in range(1):
        # seems like extending rules is affecting spark behaviour
        rules.extend(rules)

    return rules

