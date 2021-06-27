from enum import Enum


class TraversalType(Enum):
    pass


class DepthFirst(TraversalType):
    InOrder = 1
    PreOrder = 2
    PostOrder = 3


class BreathFirst(TraversalType):
    LevelOrder = 1


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
        print(" " * level, self.data)
        if (self.left):
            self.left.pp(level + 1)
        if (self.right):
            self.right.pp(level + 1)

    def dftraverse(self, traversalOrder: TraversalType, traversalList):
        if traversalOrder == DepthFirst.PreOrder:
            self.__dftraverse_preorder(traversalList)
        if traversalOrder == DepthFirst.InOrder:
            self.__dftraverse_inorder(traversalList)
        if traversalOrder == DepthFirst.PostOrder:
            self.__dftraverse_postorder(traversalList)

    def __dftraverse_preorder(self, traversalList):
        if self:
            traversalList.extend([self.data])
            if self.left:
                self.left.dftraverse(DepthFirst.PreOrder, traversalList)
            if self.right:
                self.right.dftraverse(DepthFirst.PreOrder, traversalList)

    def __dftraverse_inorder(self, traversalList):
        if self:
            if self.left:
                self.left.dftraverse(DepthFirst.InOrder, traversalList)
            traversalList.extend([self.data])
            if self.right:
                self.right.dftraverse(DepthFirst.InOrder, traversalList)

    def __dftraverse_postorder(self, traversalList):
        if self:
            if self.left:
                self.left.dftraverse(DepthFirst.PostOrder, traversalList)
            if self.right:
                self.right.dftraverse(DepthFirst.PostOrder, traversalList)
            traversalList.extend([self.data])

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

    dfTraversalList = []
    tree = KPTree(1)
    left = KPTree(2)
    right = KPTree(3)
    tree.add_left(left)
    tree.add_right(right)
    left.add_left(KPTree(4))
    left.add_right(KPTree(5))
    for traversalType in [DepthFirst.InOrder, DepthFirst.PreOrder, DepthFirst.PostOrder]:
        dfTraversalList = []
        tree.dftraverse(traversalType, dfTraversalList)
        print(traversalType, dfTraversalList)

    bfTraversalList = []
    tree.bftraverse(bfTraversalList)
    print(bfTraversalList)
