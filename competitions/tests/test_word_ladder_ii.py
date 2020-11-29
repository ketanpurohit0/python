import src.word_ladder_ii as ws2
from typing import List
import itertools


def test_dfs2():

    tree = {
        'hit': ['hot'],
        'hot': ['dot', 'lot'],
        'dot': ['dog'],
        'lot': ['log'],
        'dog': ['cog'],
        'log': ['cog'],
        'cog': []
    }

    endWord = 'cog'

    visited: List[str] = list()
    print("dfs2:", ws2.dfs2(tree, visited, "hit"))
    longest = max(map(len, ws2.visitedList))
    expected = [
        ["hit", "hot", "dot", "dog", "cog"],
        ["hit", "hot", "lot", "log", "cog"]
    ]

    result = []
    result.clear()
    for ll in filter(lambda x: len(x) == longest, ws2.visitedList):
        result.append(ll)
    assert(result == expected)

    result.clear()
    for ll in filter(lambda x: len(x) and x[-1] == endWord, ws2.visitedList):
        result.append(ll)
    assert(result == expected)


def test_methods():
    beginWord = "hit"
    endWord = "cog"
    wordList = ["hot", "dot", "dog", "lot", "log", "cog"]
    s = ws2.Solution()
    s.findLadders(beginWord, endWord, wordList)
    assert(s.wordListDiff("hit", wordList) == [1, 2, 3, 2, 3, 3])
    assert(s.wordSelector([1, 2, 3, 4], 1) == [True, False, False, False])
    assert(s.wordSelector([1, 2, 3, 4], 4) == [False, False, False, True])
    assert(s.wordSelector([1, 2, 3, 4], 99) == [False, False, False, False])
    assert(["hot"] == [x for x in itertools.compress(wordList, [True, False, False, False, False, False])])


def test_remove_word():
    beginWord = "hit"
    endWord = "cog"
    wordList = ["hot", "dot", "dog", "lot", "log", "cog"]
    s = ws2.Solution()
    newList = s.removeWord(beginWord, wordList)
    assert(newList == wordList)
    newList = s.removeWord(endWord, newList)
    assert(newList == ["hot", "dot", "dog", "lot", "log"])
