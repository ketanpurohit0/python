from enum import Enum
from typing import List

class TraversalType(Enum):
    PREORDER = 1
    INORDER = 2
    POSTORDER = 3


class Tree:
    pass


class Tree:

    @classmethod
    def reconstruct_from_preorder(cls, preorder: List[int]) -> Tree:
        # page 130 - following class method from elements of programming interviews
        def reconstruct_from_preorder_inner(preorder_iter) -> Tree:
            subtree_key = next(preorder_iter)
            if subtree_key is None:
                return None

            left_subtree = reconstruct_from_preorder_inner(preorder_iter)
            right_subtree = reconstruct_from_preorder_inner(preorder_iter)
            return Tree(subtree_key, left_subtree, right_subtree)

        return reconstruct_from_preorder_inner(iter(preorder))

    def __init__(self, value: int, left_child, right_child):
        self.node_value = value
        self.left_child = left_child
        self.right_child = right_child

    def set_left_child(self, left_child):
        self.left_child = left_child

    def set_right_child(self, right_child):
        self.right_child = right_child

    def left_size(self):
        if (self.left_child):
            return self.node_value + self.left_child.left_size()
        else:
            return self.node_value

    def right_size(self):
        if (self.right_child):
            return self.node_value + self.right_child.right_size()
        else:
            return self.node_value

    def left_height(self):
        if (self.left_child):
            return 1 + self.left_child.left_height()
        else:
            return 0

    def right_height(self):
        if (self.right_child):
            return 1 + self.right_child.right_height()
        else:
            return 0

    def dfs(self, mode: TraversalType):

        result = []

        def traverse(node):
            if node:
                result.extend(node.dfs(mode))
            else:
                result.append(None)

        if mode == TraversalType.INORDER:
            traverse(self.left_child)
            result.append(self.node_value)
            traverse(self.right_child)
        elif mode == TraversalType.PREORDER:
            result.append(self.node_value)
            traverse(self.left_child)
            traverse(self.right_child)
        else:
            traverse(self.left_child)
            traverse(self.right_child)
            result.append(self.node_value)

        # if self.left_child:
        #     result.extend(self.left_child.dfs())
        # else:
        #     result.append(None)
        #
        # if self.right_child:
        #     result.extend(self.right_child.dfs())
        # else:
        #     result.append(None)
        return result

    def bfs(self):

        def valueof(node):
            if node:
                return node.node_value
            else:
                return None
        from collections import deque
        flat = deque()
        flat.append(self)
        result = []
        while len(flat):
            t = flat.pop()
            result.append(valueof(t))

            if t:
                if t.left_child:
                    flat.appendleft(t.left_child)
                else:
                    flat.appendleft(None)
                if t.right_child:
                    flat.appendleft(t.right_child)
                else:
                    flat.appendleft(None)

        return result


if __name__ == "__main__":
    root = Tree(1, None, None)
    level1L = Tree(2, None, None)
    level1R = Tree(3, None, None)
    Level2L1 = Tree(2.1, None, None)
    Level2L2 = Tree(2.2, None, None)
    Level2L3 = Tree(3.1, None, None)

    level1R.set_left_child(Level2L3)
    level1L.set_left_child(Level2L1)
    level1L.set_right_child(Level2L2)
    root.set_left_child(level1L)
    root.set_right_child(level1R)

    print(root.right_size())
    print(root.left_size())

    print(root.right_height())
    print(root.left_height())

    print(root.dfs(TraversalType.PREORDER))
    print(root.dfs(TraversalType.INORDER))
    print(root.dfs(TraversalType.POSTORDER))
    print(root.bfs())

    preorder = root.dfs(TraversalType.PREORDER)
    rebuiltTree = Tree.reconstruct_from_preorder(preorder)
    rebuiltTree_preorder = rebuiltTree.dfs(TraversalType.PREORDER)
    print(preorder)
    print(rebuiltTree_preorder)