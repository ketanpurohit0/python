# https://leetcode.com/problems/word-ladder-ii/
from collections import defaultdict, deque
from typing import List, Set, Dict


def dfs(tree, visited: Set, rootNodeKey):
    if (rootNodeKey in tree) and rootNodeKey not in visited:
        print(rootNodeKey)
        visited.append(rootNodeKey)
        for c in tree[rootNodeKey]:
            dfs(tree, visited, c)
    return visited


visitedList = [[]]
def dfs2(tree, visited: List, rootNodeKey):
    # print(rootNodeKey)
    visited.append(rootNodeKey)
    for c in tree[rootNodeKey]:
        if c not in visited:
            dfs2(tree, visited.copy(), c)
    visitedList.append(visited)
    return visited


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
    return visited


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
    # add a root
    for w in wordList:
        if (w[0] not in tree['*']):
            tree['*'].append(w[0])
    return tree


class Solution:
    def findLadders(self, beginWord: str, endWord: str, wordList: List[str]) -> List[List[str]]:
        if endWord in wordList:
            tree = buildTree(wordList)
            print(tree)

            visited: List[str] = list()
            print("bfs:", bfs(tree, visited, "*"))
            visited: List[str] = list()
            print("dfs:", dfs(tree, visited, "*"))
        else:
            return []


if __name__ == '__main__':
    beginWord = "hit"
    endWord = "cog"
    wordList = ["hot", "dot", "dog", "lot", "log", "cog"]
    s = Solution()
    s.findLadders(beginWord, endWord, wordList)

    # wordList = ["hot","dot","dog","lot","log"]
    # s.findLadders(beginWord, endWord, wordList)

    tree = {
        'hit': ['hot'],
        'hot': ['dot', 'lot'],
        'dot': ['dog'],
        'lot': ['log'],
        'dog': ['cog'],
        'log': ['cog'],
        'cog': []
    }

    visited: List[str] = list()
    print("bfs:", bfs(tree, visited, "hit"))
    visited: List[str] = list()
    print("dfs:", dfs(tree, visited, "hit"))
    visited: List[str] = list()
    print("dfs2:", dfs2(tree, visited, "hit"))
    print(visitedList)
    longest = max(map(len, visitedList))
    for l in filter(lambda x: len(x) == longest,visitedList):
        print(l)

