# https://leetcode.com/problems/word-ladder-ii/
from collections import defaultdict, deque
from typing import List, Set, Dict


def dfs(tree, visited: Set, rootNodeKey):
    if (rootNodeKey in tree) and rootNodeKey not in visited:
        print(rootNodeKey)
        visited.add(rootNodeKey)
        for c in tree[rootNodeKey]:
            dfs(tree, visited, c)


def bfs(tree, visited: List, rootNodeKey):
    visitChildrenOf: List = []
    if (rootNodeKey in tree) and rootNodeKey not in visited:
        visited.append(rootNodeKey)
        visitChildrenOf.append(rootNodeKey)
        while visitChildrenOf:
            v = visitChildrenOf.pop(0)
            for c in tree[v]:
                if c not in visited:
                    visited.append(c)
                    visitChildrenOf.append(c)
                    # print("appended", c)
    print(visited)


def buildTree(wordList: List[str]) -> Dict[str, List[str]]:
    # tree: Dict[str, List[str]] = {}
    tree = defaultdict(list)

    for w in wordList:
        dw = deque(w)
        dw.rotate(-1)
        dw.pop()
        dw.append(None)
        for first, second in zip(w, dw):
            try:
                if (second not in tree[first]):
                    tree[first].append(second)
            except StopIteration:
                pass

    return tree


class Solution:
    def findLadders(self, beginWord: str, endWord: str, wordList: List[str]) -> List[List[str]]:
        if endWord in wordList:
            tree = buildTree(wordList)
            visited: List[str] = list()
            bfs(tree, visited, "h")
            visited: Set[str] = set()
            dfs(tree, visited, "h")
            print(tree)
        else:
            return []


if __name__ == '__main__':
    beginWord = "hit"
    endWord = "cog"
    wordList = ["hot", "dot", "dog", "lot", "log", "cog"]
    buildTree(wordList)
    s = Solution()
    s.findLadders(beginWord, endWord, wordList)

    # wordList = ["hot","dot","dog","lot","log"]
    # s.findLadders(beginWord, endWord, wordList)