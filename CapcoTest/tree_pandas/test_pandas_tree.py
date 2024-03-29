import hashlib
import itertools
import string
import time
import unittest

import pandas as pd
from hypothesis import given, strategies
from hypothesis.strategies import integers, text
from pydantic import BaseModel, Field

from TreeStruct import AccountTree

tree_data = {
    "PARENT": ["root", "root", "1", "1", "1.2"],
    "CHILD": ["1", "2", "1.1", "1.2", "1.2.1"],
    "ALLOCATE_PCT": [50, 50, 60, 40, 100],
}

more_tree_data = {
    "PARENT": ["root", "root", "B2", "root", "B3", "C3", "D3"],
    "CHILD": ["B1", "B2", "C2", "B3", "C3", "D3", "D3.1"],
    "ALLOCATE_PCT": [50] * 7,
}

real_sample_tree_data = {
    "PRNT ACNT NO": [
        None,
        "12330",
        "12330",
        "12330",
        "46460",
        "46460",
        "46460",
        "46460",
        None,
        "33717",
        "33717",
        "33717",
        "33717",
        "33717",
        "33717",
        "33717E",
        "33717E",
        None,
        "48506",
        "48506",
        "48506",
        "48506",
    ],
    "ACNT NO": [
        "12330",
        "12330A",
        "12330B",
        "12330C",
        "46460",
        "46460A",
        "46460B",
        "46460C",
        "33717",
        "33717A",
        "33717D",
        "33717C",
        "33717F",
        "33717B",
        "33717E",
        "33717G",
        "33717F",
        "48506",
        "48506A",
        "48506B",
        "48506C",
        "48506D",
    ],
    "ALLOC PERC": [
        100.0,
        18.27,
        81.71,
        0.02,
        None,
        18.27,
        81.71,
        0.02,
        None,
        33.97,
        5.04,
        1.14,
        0.01,
        6.24,
        53.60,
        98.49,
        1.51,
        None,
        21.05,
        0.0,
        21.85,
        57.10,
    ],
}


def sha256sum(filename):
    # https: // stackoverflow.com / questions / 22058048 / hashing - a - file - in -python
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()


class User(BaseModel):
    name: str = Field(min_length=3)
    age: int = Field(gt=72)


class TestTreePandas(unittest.TestCase):
    def test_df_with_parent_child_link(self):
        tree_df = pd.DataFrame(tree_data)
        group_df = tree_df.groupby(by="PARENT").agg({"ALLOCATE_PCT": sum})
        mask = group_df["ALLOCATE_PCT"] == 100
        self.assertTrue(all(mask))
        # with open("group_df.pickle", "wb") as f:
        #

    def test_tree(self):
        tree_df = pd.DataFrame(tree_data)[["PARENT", "CHILD", "ALLOCATE_PCT"]]
        tree = AccountTree("root", 0)
        tree_df.apply(lambda r: tree.insert(r.PARENT, r.CHILD, r.ALLOCATE_PCT), axis=1)
        self.assertEqual(len(tree_df), tree.node_count() - 1)
        tree.dump()
        print("1.2", tree.path_of("1.2"))
        print("1.2.1", tree.path_of("1.2.1"))
        print("root", tree.path_of("root"))

    def test_df_second_tree(self):
        tree_df = pd.DataFrame(more_tree_data)
        tree = AccountTree("root", 0)
        tree_df.apply(lambda r: tree.insert(r.PARENT, r.CHILD, r.ALLOCATE_PCT), axis=1)
        tree.dump()
        self.assertEqual(len(tree_df), tree.node_count() - 1)
        print(tree.root_account())
        print("C3", tree.path_of("C3"))
        print("D3", tree.path_of("D3"))
        print("D3.1", tree.path_of("D3.1"))
        print("C2", tree.path_of("C2"))
        print("Dxx", tree.path_of("Dxx"))

    def test_real_sample_data(self):
        tree_df = pd.DataFrame(real_sample_tree_data)
        # clean
        # None in PRNT ACNT NO implies 'root'
        # If PRNT ACNT NO and ACNT NO are same, then PRNT ACNT NO is set to 'root' (implied it is a parent)
        tree_df["PRNT ACNT NO"] = tree_df["PRNT ACNT NO"].apply(lambda r: r or "root")
        tree_df["PRNT ACNT NO"] = tree_df.apply(
            lambda r: "root"
            if r["PRNT ACNT NO"] == r["ACNT NO"]
            else r["PRNT ACNT NO"],
            axis=1,
        )
        tree_df["ALLOC PERC"] = tree_df["ALLOC PERC"].fillna(100.0)
        tree_root = AccountTree(account_code="root", allocation_rate=100.0)
        tree_df.apply(
            lambda r: tree_root.insert(
                r["PRNT ACNT NO"], r["ACNT NO"], r["ALLOC PERC"],
            ),
            axis=1,
        )

        # make assertions
        tree_root.allocated_rate_calculation()
        self.assertTrue(
            tree_root.verify_sum_of_all_child_allocation_rates(reveal_node=True),
        )
        self.assertTrue(tree_root.verify_sum_of_child_allocations(reveal_node=True))
        for amount, multiplier in itertools.product([7, 313, 13, 31, 1], [1, 3, 5, 7, 11]):
            with self.subTest(amount * multiplier):
                tree_root.allocate_amount(amount * multiplier, ndp=2)
                flatten_tree = tree_root.flatten()
                self.assertTrue(
                    tree_root.verify_sum_of_child_allocations(reveal_node=True),
                )
                # +1 because of the dummy root
                self.assertEqual(len(tree_df) + 1, len(flatten_tree))
                pd.DataFrame(flatten_tree,
                                             columns=["Level", "PRNT ACNT NO", "ALLOC RATE", "OVERALL ALLOC RATE",
                                                      "ALLOC_AMT"])

    def test_increment_amount(self):
        tree_df = pd.DataFrame(real_sample_tree_data)
        # clean
        # None in PRNT ACNT NO implies 'root'
        # If PRNT ACNT NO and ACNT NO are same, then PRNT ACNT NO is set to 'root' (implied it is a parent)
        tree_df["PRNT ACNT NO"] = tree_df["PRNT ACNT NO"].apply(lambda r: r or "root")
        tree_df["PRNT ACNT NO"] = tree_df.apply(
            lambda r: "root"
            if r["PRNT ACNT NO"] == r["ACNT NO"]
            else r["PRNT ACNT NO"],
            axis=1,
        )
        tree_df["ALLOC PERC"] = tree_df["ALLOC PERC"].fillna(100.0)
        tree_root = AccountTree(account_code="root", allocation_rate=100.0)
        tree_df.apply(
            lambda r: tree_root.insert(
                r["PRNT ACNT NO"], r["ACNT NO"], r["ALLOC PERC"],
            ),
            axis=1,
        )

        # make assertions
        tree_root.allocated_rate_calculation()
        self.assertTrue(
            tree_root.verify_sum_of_all_child_allocation_rates(reveal_node=True),
        )
        self.assertTrue(tree_root.verify_sum_of_child_allocations(reveal_node=True))
        total_increment = 0
        for amount, multiplier in itertools.product([7, 313, 13, 31, 1], [1, 3, 5, 7, 11]):
            with self.subTest(amount * multiplier):
                tree_root.increment_amount(amount * multiplier, ndp=2)
                total_increment += amount * multiplier
                flatten_tree = tree_root.flatten()
                self.assertTrue(
                    tree_root.verify_sum_of_child_allocations(reveal_node=True),
                )
                # +1 because of the dummy root
                self.assertEqual(len(tree_df) + 1, len(flatten_tree))
                pd.DataFrame(flatten_tree,
                                             columns=["Level", "PRNT ACNT NO", "ALLOC RATE", "OVERALL ALLOC RATE",
                                                      "ALLOC_AMT"])

        self.assertEqual(total_increment, tree_root.allocation_amount)

    def test_vectorized_ops(self):
        size = 10_000
        df = pd.DataFrame({"A": [5] * size})

        ts = time.perf_counter()
        df["AC"] = df.A.map(lambda r: r * 5)
        print(time.perf_counter() - ts)

        def multBy(s: pd.Series, n: int) -> pd.Series:
            return s * n

        def multBy2(pd_dataframe: pd.DataFrame, col: str, n: int) -> pd.Series:
            return pd_dataframe[col] * n

        ts = time.perf_counter()
        df["AC1"] = multBy(df.A, 5)
        print(time.perf_counter() - ts)

        ts = time.perf_counter()
        df["AC2"] = df.apply(lambda r: r["A"] * 5, axis=1)
        print(time.perf_counter() - ts)

        ts = time.perf_counter()
        df["AC3"] = multBy2(df, "A", 5)
        print(time.perf_counter() - ts)

        self.assertTrue(all(df["AC"] == df["AC1"]))
        self.assertTrue(all(df["AC"] == df["AC2"]))
        self.assertTrue(all(df["AC"] == df["AC3"]))

        # AC1: 0.06376220000000021 fastest multBy
        # AC2: 76.0665882 - slowest apply
        # AC3: 0.07285819999999887 - 2nd fastest multBy2

    @given(age=integers(), name=text())
    def test_hyp_pydantic(self, age: int, name: str):
        self.assertTrue(isinstance(age, int))

    @given(py=strategies.builds(User, name=text(min_size=3, alphabet=string.ascii_uppercase), age=integers(min_value=73)))
    def test_hyp_pydantic2(self, py: User):
        print(py)
        self.assertTrue(isinstance(py, User))

if __name__ == "__main__":
    unittest.main()
