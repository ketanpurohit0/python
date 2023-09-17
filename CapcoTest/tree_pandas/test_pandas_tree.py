import unittest
from collections import defaultdict

import pandas as pd
import pickle
import hashlib

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
        def tree(): return defaultdict(tree)

        def add(t: tree(), path):
            for node in path:
                t = t[node]

        def get_node(t: tree(), path):
            for node in path:
                t = t[node]
            return t


        root = tree()
        add(root, "A>B".split('>'))

        node = get_node(root, "A>B".split('>'))

        pass


if __name__ == '__main__':
    unittest.main()
