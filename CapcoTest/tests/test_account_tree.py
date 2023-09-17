import unittest

from TreeStruct import AccountTree


class TestAccountTree(unittest.TestCase):

    def test_create_root(self):
        tree = AccountTree(account_code=None)
        self.assertIsNone(tree.parent_account)

    def test_create_non_root(self):
        account_code = "123"
        tree = AccountTree(account_code=account_code)
        self.assertEqual(tree.parent_account, account_code)

    def test_insert_at_root(self):
        tree = AccountTree(account_code=None)
        parent_account, child_account = (None, "123")
        was_added = tree.insert(parent_account, child_account=child_account)
        self.assertTrue(was_added)
        pa: AccountTree = tree.find(account_code = parent_account)
        ca: AccountTree = tree.find(account_code = child_account)
        self.assertIsNotNone(pa)
        self.assertIsNotNone(ca)
        self.assertIsNone(pa.parent_account)
        self.assertTrue(pa.children_accounts)
        self.assertTrue(ca.parent_account, child_account)

    def test_insert_generally(self):
        tree = AccountTree(account_code=None)
        parent_account, child_account = (None, "123")
        _ = tree.insert(parent_account, child_account=child_account)
        parent_account, child_account = ("123", "123/1")
        was_added = tree.insert(parent_code=parent_account, child_account=child_account)
        self.assertTrue(was_added)
        self.assertIsNotNone(tree.find(None))
        self.assertIsNotNone(tree.find("123"))
        self.assertIsNotNone(tree.find("123/1"))

    def test_insert_and_find_non_existing(self):
        tree = AccountTree(account_code=None)
        parent_account, child_account = (None, "123")
        _ = tree.insert(parent_account, child_account=child_account)
        parent_account, child_account = ("123", "123/1")
        was_added = tree.insert(parent_code=parent_account, child_account=child_account)
        self.assertTrue(was_added)
        self.assertIsNone(tree.find("does_not_exist"))



if __name__ == '__main__':
    unittest.main()
