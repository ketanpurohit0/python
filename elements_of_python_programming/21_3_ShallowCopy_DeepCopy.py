import copy

if __name__ == '__main__':

    base_object = [[0], [1], [2]]
    new_object = base_object
    base_object[0] = ["7"]

    print(base_object)
    print(new_object)  # both affected: not copied simple a ref to the target

    base_object = [[0, 1], [1, 2], [2, 3]]
    new_object = copy.copy(base_object)
    new_object[1] = ["8"]

    print(base_object)
    print(new_object)  # only new affected (even though it is a shallow copy)

    base_object = [[0, 1], [1, 2], [2, 3]]
    new_object = copy.deepcopy(base_object)
    new_object[1] = ["9"]

    print(base_object)
    print(new_object)  # only new affected - expected due to deep copy

    base_object = [[1, 2, 3], [4, 5, 6]]
    new_object = copy.copy(base_object)
    new_object[0][0] = 0

    print(base_object)
    print(new_object)  # both modified, we are amending a single element in a shallow copy

    base_object = [[1, 2, 3], [4, 5, 6]]
    new_object = copy.deepcopy(base_object)
    new_object[0][0] = 0

    print(base_object)
    print(new_object)  # only new modified
