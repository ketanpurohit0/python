def solution(T):

    seasons = ["WINTER", "SPRING", "SUMMER", "AUTUMN"]
    temp_ranges_per_season = []
    step_size = len(T)//4
    for r in range(4):
        # take slices and get max and min per slice from left to right
        max_for_season_r = max(T[r*step_size:r*step_size+(step_size-1)+1])
        min_for_season_r = min(T[r*step_size:r*step_size+(step_size-1)+1])
        # calculate temperature range
        range_for_season_r = max_for_season_r-min_for_season_r
        # add temperature range to collection
        temp_ranges_per_season.append(range_for_season_r)

    # get the index of the highest range
    index_of_season_with_highest_range = temp_ranges_per_season.index(max(temp_ranges_per_season))
    # return the season name
    return seasons[index_of_season_with_highest_range]

if __name__ == '__main__':
    inputs = [
        [-3, -14, -5, 7, 8, 42, 8, 3],
        [2, -3, 3, 1, 10, 8, 2, 5, 13, -5, 3, -18]
    ]

    for i in inputs:
        print(i, "->", solution(i))
