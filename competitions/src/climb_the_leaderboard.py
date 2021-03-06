# https://www.hackerrank.com/challenges/climbing-the-leaderboard/problem

from typing import List, Set


def climbingLeaderboard(ranked, player):
    leaderboard = {}
    rank = 1
    # set-up initial
    for score in sorted(ranked, reverse=True):
        if (score not in leaderboard):
            leaderboard[score] = rank
            rank += 1

    ll = sorted(list(leaderboard.keys()))
    lenll = len(ll)
    # print(ll)
    # print(player)
    r = []
    for score in player:
        search_index = 0
        # pos = len([x for x in ll[search_index:] if x <= score])
        pos = sum(map(lambda x: x <= score, ll[search_index:]))
        # print(search_index, pos, lenll-pos+1)
        r.append(lenll - pos + 1)
        search_index = pos
    # walk the board - denser code than I would like
    # print(r)
    # r = [1 + len([x for x in ll if x > score]) for score in player]
    # print(r)
    return r


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


if __name__ == '__main__':

    graph = {
        'A': ['B', 'C'],
        'B': ['D', 'E'],
        'C': ['F'],
        'D': [],
        'E': ['F'],
        'F': []
    }
    visited = set()

    dfs(graph, visited, "A")

    visited = list()
    bfs(graph, visited, "A")

    graph = {
        '*': ['A', 'D', 'E'],
        'A': ['B'],
        'B': ['C'],
        'C': [],
        'D': [],
        'E': ['F'],
        'F': []
    }
    for r in "*ADE":
        visited = list()
        bfs(graph, visited, r)

    exit
    ranked = [100, 100, 50, 40, 40, 20, 10]
    player = [5, 25, 50, 120]

    climbingLeaderboard(ranked, player)

    ranked = [100, 90, 90, 80, 75, 60]
    player = [50, 65, 77, 90, 102]

    climbingLeaderboard(ranked, player)
