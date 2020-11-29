# https://leetcode.com/problems/word-ladder-ii/
from collections import defaultdict, deque
from typing import List, Set, Dict
import itertools


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
        visited = []
        if endWord in wordList:
            queue = []
            queue.append(beginWord)
            while queue:
                print("Q:", queue)
                print("V:", visited)
                wordToSearch = queue.pop(0)
                visited.append(wordToSearch)
                selector = self.wordSelector(self.wordListDiff(wordToSearch, wordList), 1)
                print(wordToSearch, selector)
                if any(selector):
                    # found at least one word with 1 letter diff
                    queue.extend([i for i in itertools.compress(wordList, selector) if i not in queue])
                    wordList = self.removeWord(wordToSearch, wordList)
        else:
            return []

    def wordDiff(self, w1: str, w2: str):
        return sum(l1 != l2 for l1, l2 in zip(w1, w2))

    def wordListDiff(self, beginWord: str, wordList: List[str]) -> List[int]:
        return [self.wordDiff(beginWord, x)for x in wordList]

    def wordSelector(self, wordListDiff: List[int], count: int):
        return [x == count for x in wordListDiff]

    def removeWord(self, word: str, wordList: List[str]):
        for (i, w) in enumerate(wordList):
            if w == word:
                wordList.pop(i)
                break
        return wordList


if __name__ == '__main__':
    beginWord = "hit"
    endWord = "cog"
    wordList = ["hot", "dot", "dog", "lot", "log", "cog"]
    s = Solution()
    s.findLadders(beginWord, endWord, wordList)
    # print(s.wordListDiff("hit", wordList))
    # print(s.wordSelector([1, 2, 3, 4], 1))
    # print(s.wordSelector([1, 2, 3, 4], 4))
    # x = itertools.compress(wordList, [True, False, False, False, False, False])
    # print([i for i in x])
