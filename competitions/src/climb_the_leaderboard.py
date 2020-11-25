# https://www.hackerrank.com/challenges/climbing-the-leaderboard/problem

def climbingLeaderboard(ranked, player):
    leaderboard = {}
    rank = 1
    # set-up initial
    for score in sorted(ranked, reverse=True):
        if (score not in leaderboard):
            leaderboard[score] = rank
            rank += 1

    ll = sorted(list(leaderboard.keys()))
    print(ll)
    print(player)
    for score in player:
        print(len([x for x in ll if x <= score]))
    # walk the board - denser code than I would like
    r = [1 + len([x for x in ll if x > score]) for score in player]
    return r


if __name__ == '__main__':
    ranked = [100, 100, 50, 40, 40, 20, 10]
    player = [5, 25, 50, 120]

    climbingLeaderboard(ranked, player)

    ranked = [100, 90, 90, 80, 75, 60]
    player = [50, 65, 77, 90, 102]

    climbingLeaderboard(ranked, player)
