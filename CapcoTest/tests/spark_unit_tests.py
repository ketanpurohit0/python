import unittest
from typing import Any

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from datetime import date, datetime
import great_expectations as ge
import json


class MySparkTests(unittest.TestCase):
    def test_dataframe_expectations(self):
        spark = SparkSession.builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        df = spark.createDataFrame(
            [
                Row(
                    a=1,
                    b=2.0,
                    c="foo1",
                    d=date(2000, 1, 1),
                    e=datetime(2000, 1, 1, 12, 0),
                ),
                Row(
                    a=2,
                    b=3.0,
                    c="foo2",
                    d=date(2000, 2, 1),
                    e=datetime(2000, 1, 2, 12, 0),
                ),
                Row(
                    a=4,
                    b=5.0,
                    c="string3",
                    d=date(2000, 3, 1),
                    e=datetime(2000, 1, 3, 12, 0),
                ),
            ]
        )

        context = ge.get_context()
        asset = context.data_sources.add_spark("spark").add_dataframe_asset(
            "data_quality_test"
        )

        validator = context.get_validator(
            batch_request=asset.build_batch_request(options={"dataframe": df})
        )
        result_format = {"result_format": "COMPLETE", "include_unexpected_rows": True}
        result = validator.expect_column_values_to_be_in_set(
            "a", [1, 2, 4, 5], result_format=result_format
        )
        print(result["success"])
        result = validator.expect_table_row_count_to_equal(3)
        print(result["success"])
        result = validator.expect_column_to_exist("a")
        print(result["success"])
        result = validator.expect_column_values_to_not_be_null("b")
        print(result["success"])
        result = validator.expect_column_values_to_be_in_type_list(
            "b", ["LongType", "DoubleType"]
        )
        print(result["success"])
        result = validator.expect_column_max_to_be_between("b", 1, 10)
        print(result["success"])
        result = validator.expect_column_values_to_match_regex("c", regex="^string.*", condition_parser="spark",
                                                               row_condition='a > 2')
        print(result["success"])
        result = validator.expect_column_values_to_match_regex("c", regex="^foo.*", condition_parser="spark",
                                                               row_condition='a <= 2', result_format=result_format)
        print("L1>", result["success"])
        # Repeat with argv, argc
        argc = ["c"]
        argv = {"regex": "^foo.*", "condition_parser": "spark", "row_condition": 'a <= 2'}
        result1 = validator.expect_column_values_to_match_regex(*argc, **argv)
        print("L2>", result1["success"])
        # Repeat with dynamic code
        result2: Any = None
        expr = "validator.expect_column_values_to_match_regex(*argc, **argv)"
        result2 = eval(expr)
        print("L3>", result2["success"])
        # Repeat without argc
        argv = {"column": "c", "regex": "^foo.*", "condition_parser": "spark", "row_condition": 'a <= 2'}
        expr2 = "validator.expect_column_values_to_match_regex(**argv)"
        result3 = eval(expr2)
        print("L4>", result3["success"])
        self.assertEqual(result, result1)
        self.assertEqual(result, result2)
        self.assertEqual(result, result3)
        # result = validator.expect_column_values_to_be_dateutil_parseable("d")
        # print("date", result["success"])
        # result = validator.expect_column_values_to_be_dateutil_parseable("e")
        # print("datetime", result["success"])

        validator.save_expectation_suite("mytests.json")
        with open("mytests.json") as fp:
            s = json.load(fp)
            suite_result = validator.validate(expectation_suite=s)
        print(result)

    def test_from_config(self):
        pass


if __name__ == "__main__":
    unittest.main()
