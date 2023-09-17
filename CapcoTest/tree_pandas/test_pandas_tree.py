import unittest
from collections import defaultdict

import pandas as pd
import pickle
import hashlib

from TreeStruct import AccountTree

tree_data = {
    "PARENT": ["root","root","1", "1", "1.2"],
    "CHILD": ["1","2","1.1", "1.2", "1.2.1"],
    "ALLOCATE_PCT": [50, 50, 60, 40, 100]
}

def sha256sum(filename):
    # https: // stackoverflow.com / questions / 22058048 / hashing - a - file - in -python
    h  = hashlib.sha256()
    b  = bytearray(128*1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        for n in iter(lambda : f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()

class TestTreePandas(unittest.TestCase):
    def test_df_with_parent_child_link(self):
        tree_df = pd.DataFrame(tree_data)
        group_df = tree_df.groupby(by="PARENT").agg({"ALLOCATE_PCT":sum})
        with open("group_df.pickle", "wb") as f:
            pickle.dump(group_df, f)
            # print(hashlib.file_digest(f, 'sha256').hexdigest())

        print(sha256sum("group_df.pickle"))

    def test_tree(self):
        tree_df = pd.DataFrame(tree_data)[["PARENT", "CHILD"]]
        tree = AccountTree("root")
        tree_df.apply(lambda r: tree.insert(r.PARENT, r.CHILD), axis=1)
        self.assertEqual(tree.node_count() - 1, len(tree_df))



if __name__ == '__main__':
    unittest.main()
