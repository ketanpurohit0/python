import unittest
from collections import defaultdict

import pandas as pd
import pickle
import hashlib

from TreeStruct import AccountTree

tree_data = {
    "PARENT": ["root", "root", "1", "1", "1.2"],
    "CHILD": ["1", "2", "1.1", "1.2", "1.2.1"],
    "ALLOCATE_PCT": [50, 50, 60, 40, 100],
}

more_tree_data = {
    "PARENT": ["root","root","B2","root","B3","C3", "D3"],
    "CHILD": [
        "B1", "B2", "C2", "B3", "C3", "D3", "D3.1"
    ],
    "ALLOCATE_PCT": [50] * 7,
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


class TestTreePandas(unittest.TestCase):
    def test_df_with_parent_child_link(self):
        tree_df = pd.DataFrame(tree_data)
        group_df = tree_df.groupby(by="PARENT").agg({"ALLOCATE_PCT": sum})
        mask = group_df['ALLOCATE_PCT'] == 100
        self.assertTrue(all(mask))
        # with open("group_df.pickle", "wb") as f:
        #     pickle.dump(group_df, f)
        #     # print(hashlib.file_digest(f, 'sha256').hexdigest())
        #
        # print(sha256sum("group_df.pickle"))

    def test_tree(self):
        tree_df = pd.DataFrame(tree_data)[["PARENT", "CHILD", "ALLOCATE_PCT"]]
        tree = AccountTree("root", 0)
        tree_df.apply(lambda r: tree.insert(r.PARENT, r.CHILD, r.ALLOCATE_PCT), axis=1)
        self.assertEqual(len(tree_df), tree.node_count() - 1)
        tree.dump()
        print("1.2", tree.path_of("1.2"))
        print("1.2.1", tree.path_of("1.2.1"))
        print("root",tree.path_of("root"))


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




if __name__ == "__main__":
    unittest.main()
