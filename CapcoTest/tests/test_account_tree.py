import unittest
from typing import Any, List, Tuple

import numpy as np

from TreeStruct import AccountTree


class TestAccountTree(unittest.TestCase):

    def test_create_root(self):
        """Create a node."""
        tree = AccountTree(account_code=None, allocation_rate=1)
        self.assertIsNone(tree.parent_account)

    def test_create_non_root(self):
        """Create a node with a non-None value."""
        account_code = "123"
        tree = AccountTree(account_code=account_code, allocation_rate=1)
        self.assertEqual(tree.parent_account, account_code)

    def test_insert_at_root(self):
        """Create a tree and insert nodes."""
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
        """Attempt to add an account such that the parent does not exist in the first place."""
        tree = AccountTree(account_code=None, allocation_rate=1)
        parent_account, child_account = (None, "123")
        was_added = tree.insert(parent_account, child_account=child_account, allocation_rate=1)
        self.assertTrue(was_added)
        parent_account, child_account = ("123f", "123/1")
        was_added = tree.insert(parent_code=parent_account, child_account=child_account, allocation_rate=1)
        self.assertFalse(was_added)

    def test_insert_generally(self):
        """Attempt different inserts."""
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
            100: [("root", 100.00), ("r2", 25.00), ("r2/r2.1", 25.00), ("r2/r2.1/r.2.1.5", 1.41)],
            111: [("root", 111.00), ("r2", 27.75), ("r2/r2.1", 27.75), ("r2/r2.1/r.2.1.5", 1.57)],
        }
        for allocate_amount,expect_result in test_dict.items():
            tree.allocate_amount(allocate_amount)
            for e in expect_result:
                account_code, expected_allocation = e
                tree_node = tree.find(account_code=account_code)
                self.assertTrue(np.isclose(expected_allocation, tree_node.allocation_amount, atol=0.01))


        self.assertTrue(tree.verify_sum_of_child_allocations(reveal_node=True))

    def test_increment_amount(self):
        _, tree = self.build_test_tree()

        allocations = [("r2", 27.75), ("r2", 11.25), ("r2", 1.00)]
        expected_result = ("r2", 40.00)

        for node_value_,amount_ in allocations:
            node_ = tree.find(node_value_)
            node_.increment_amount(amount_)

        account_code, expected_allocation = expected_result
        tree_node = tree.find(account_code=account_code)
        self.assertTrue(np.isclose(expected_allocation, tree_node.allocation_amount, atol=0.01))


        self.assertTrue(tree.verify_sum_of_child_allocations(reveal_node=True))


    def test_amount_reset_whole_tree(self):
        _, tree = self.build_test_tree()
        tree.allocate_amount(1_000_000)
        self.assertEqual(1_000_000, tree.sum_of_immediate_child_allocations_amount())
        tree.reset_amount()
        self.assertEqual(0, tree.sum_of_immediate_child_allocations_amount())

    def test_amount_reset_sub_node(self):
        """Allocate amount to parent, and then reset_amount on a child.
        Ensure child becomes 0 and root is unchanged.
        """
        _, tree = self.build_test_tree()
        tree.allocate_amount(1_000_000)
        self.assertTrue(tree.verify_sum_of_child_allocations())
        sub_node = tree.find("r2/r2.1/r.2.1.1")
        self.assertEqual(1_000_000, tree.sum_of_immediate_child_allocations_amount())
        sub_node.reset_amount()
        self.assertEqual(0, sub_node.sum_of_immediate_child_allocations_amount())
        self.assertNotEqual(0, tree.sum_of_immediate_child_allocations_amount())
        self.assertFalse(tree.verify_sum_of_child_allocations())

    @staticmethod
    def build_test_tree() -> Tuple[List[Any], AccountTree]:
        tree = AccountTree("root", 1)
        elements = [
            ("root", "r1", 25),
            ("root", "r2", 25),
            ("root", "r3", 25),
            ("root", "r4", 25),
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


if __name__ == "__main__":
    unittest.main()
