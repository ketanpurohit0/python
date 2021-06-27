

class KPTree:

    def __init__(self, data: str):
        self.data = data
        self.left = None
        self.right = None

    def add_left(self, node):
        self.left = node

    def add_right(self, node):
        self.right = node

    def pp(self, level: int = 0) -> None:
        print(" "* level, self.data)
        if (self.left):
            self.left.pp(level+1)
        if (self.right):
            self.right.pp(level+1)

    def dftraverse(self, traversalList):
        if self:
            traversalList.extend([self.data])
            if self.left:
                self.left.dftraverse(traversalList)
            if self.right:
                self.right.dftraverse(traversalList)

    def bftraverse(self, traversalList):
        if self:
            if self.data not in traversalList:
                traversalList.append(self.data)
            if self.left:
                traversalList.append(self.left.data)
            if self.right:
                traversalList.append(self.right.data)

            if self.left:
                self.left.bftraverse(traversalList)
            if self.right:
                self.right.bftraverse(traversalList)


if __name__ == "__main__":
    tree = KPTree("data")
    left = KPTree("left")
    right = KPTree("right")
    left.add_left(KPTree("left_left"))
    left.add_right(KPTree("left_right"))
    right.add_left(KPTree("right_left"))
    right.add_right(KPTree("right_right"))
    tree.add_left(left)
    tree.add_right(right)
    tree.pp()

    bfTraversalList = []
    tree.bftraverse(bfTraversalList)
    print(bfTraversalList)

    dfTraversalList = []
    tree.dftraverse(dfTraversalList)
    print(dfTraversalList)

    bfTraversalList = []
    tree = KPTree(1)
    left = KPTree(2)
    right = KPTree(3)
    tree.add_left(left)
    tree.add_right(right)
    left.add_left(KPTree(4))
    left.add_right(KPTree(5))
    tree.dftraverse(bfTraversalList)
    print(bfTraversalList)
