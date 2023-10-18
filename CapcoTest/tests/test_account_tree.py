import unittest
from typing import Tuple, List, Any

import numpy as np

from TreeStruct import AccountTree


class TestAccountTree(unittest.TestCase):

    def test_create_root(self):
        tree = AccountTree(account_code=None, allocation_rate=1)
        self.assertIsNone(tree.parent_account)

    def test_create_non_root(self):
        account_code = "123"
        tree = AccountTree(account_code=account_code, allocation_rate=1)
        self.assertEqual(tree.parent_account, account_code)

    def test_insert_at_root(self):
        tree = AccountTree(account_code=None, allocation_rate=1)
        parent_account, child_account = (None, "123")
        was_added = tree.insert(parent_account, child_account=child_account, allocation_rate=1)
        self.assertTrue(was_added)
        pa: AccountTree = tree.find(account_code=parent_account)
        ca: AccountTree = tree.find(account_code=child_account)
        self.assertIsNotNone(pa)
        self.assertIsNotNone(ca)
        self.assertIsNone(pa.parent_account)
        self.assertTrue(pa.children_accounts)
        self.assertTrue(ca.parent_account, child_account)

    def test_insert_where_parent_does_not_exist(self):
        tree = AccountTree(account_code=None, allocation_rate=1)
        parent_account, child_account = (None, "123")
        was_added = tree.insert(parent_account, child_account=child_account, allocation_rate=1)
        self.assertTrue(was_added)
        parent_account, child_account = ("123f", "123/1")
        was_added = tree.insert(parent_code=parent_account, child_account=child_account, allocation_rate=1)
        self.assertFalse(was_added)

    def test_insert_generally(self):
        tree = AccountTree(account_code=None, allocation_rate=1)
        parent_account, child_account = (None, "123")
        _ = tree.insert(parent_account, child_account=child_account, allocation_rate=1)
        parent_account, child_account = ("123", "123/1")
        was_added = tree.insert(parent_code=parent_account, child_account=child_account, allocation_rate=1)
        self.assertTrue(was_added)
        self.assertIsNotNone(tree.find(None))
        self.assertIsNotNone(tree.find("123"))
        self.assertIsNotNone(tree.find("123/1"))

    def test_insert_and_find_non_existing(self):
        tree = AccountTree(account_code=None, allocation_rate=1)
        parent_account, child_account = (None, "123")
        _ = tree.insert(parent_account, child_account=child_account, allocation_rate=1)
        parent_account, child_account = ("123", "123/1")
        was_added = tree.insert(parent_code=parent_account, child_account=child_account, allocation_rate=1)
        self.assertTrue(was_added)
        self.assertIsNone(tree.find("does_not_exist"))

    def test_create_arbitrary_tree(self):
        elements, tree = self.build_test_tree()
        nodes = tree.node_count() - 1
        self.assertEqual(len(elements), nodes)

        tree_node = tree.find("r2/r2.1")
        self.assertIsNotNone(tree_node)
        total_allocation_rate = tree_node.sum_of_immediate_child_allocations_rate()
        self.assertTrue(np.isclose(100, total_allocation_rate))

        for parent_account, _, _ in elements:
            with self.subTest(parent_account):
                tree_node = tree.find(account_code=parent_account)
                total_allocation_rate = tree_node.sum_of_immediate_child_allocations_rate()
                self.assertTrue(np.isclose(100, total_allocation_rate), msg=parent_account)

    def test_allocated_amount(self):
        _, tree = self.build_test_tree()

        test_dict = {
            100: [(None, 100.00), ("r2", 25.00), ("r2/r2.1", 25.00), ("r2/r2.1/r.2.1.5", 1.41)],
            111: [(None, 111.00), ("r2", 27.75), ("r2/r2.1", 27.75), ("r2/r2.1/r.2.1.5", 1.57)]
        }
        for allocate_amount,expect_result in test_dict.items():
            tree.allocate_amount(allocate_amount)
            for e in expect_result:
                account_code, expected_allocation = e
                tree_node = tree.find(account_code=account_code)
                self.assertTrue(np.isclose(expected_allocation, tree_node.allocation_amount, atol=0.01))

        tree.dump()

        self.assertTrue(tree.verify_sum_of_child_allocations(reveal_node=True))

    @staticmethod
    def build_test_tree() -> Tuple[List[Any], AccountTree]:
        tree = AccountTree(None, 1)
        elements = [
            (None, "r1", 25),
            (None, "r2", 25),
            (None, "r3", 25),
            (None, "r4", 25),
            ("r1", "r1/r1.1", 30),
            ("r1", "r1/r1.2", 70),
            ("r2", "r2/r2.1", 100),
            ("r2/r2.1", "r2/r2.1/r.2.1.1", 11.98),
            ("r2/r2.1", "r2/r2.1/r.2.1.2", 21.22),
            ("r2/r2.1", "r2/r2.1/r.2.1.3", 54.76),
            ("r2/r2.1", "r2/r2.1/r.2.1.4", 6.40),
            ("r2/r2.1", "r2/r2.1/r.2.1.5", 5.64),
        ]
        for e in elements:
            tree.insert(*e)
        return elements, tree


if __name__ == '__main__':
    unittest.main()
