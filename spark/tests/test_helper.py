import pytest
import SparkHelper as sh


@pytest.fixture
def get_dataframe():
    spark = sh.getSpark()
    dict_lst = {'letters': ['a', 'b', 'c'],
                'numbers': [10, 20, 30]}

    column_names, data = zip(*dict_lst.items())
    return spark.createDataFrame(zip(*data), column_names)


def test_self(get_dataframe):
    assert(True)
