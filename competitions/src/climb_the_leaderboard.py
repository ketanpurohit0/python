# https://www.hackerrank.com/challenges/climbing-the-leaderboard/problem

def climbingLeaderboard(ranked, player):
    leaderboard = {}
    rank = 1
    # set-up initial
    for (index, score) in enumerate(sorted(ranked, reverse=True), start=1):
        if (score not in leaderboard):
            leaderboard[score] = rank
            rank += 1

    ll = list(leaderboard.keys())
    # walk the board
    for score in player:
        nr = [x for x in ll if x > score]
        print(1+len(nr))


if __name__ == '__main__':
    ranked = [100, 100, 50, 40, 40, 20, 10]
    player = [5, 25, 50, 120]

    climbingLeaderboard(ranked, player)

    ranked = [100, 90, 90, 80, 75, 60]
    player = [50, 65, 77, 90, 102]

    climbingLeaderboard(ranked, player)
