import heapq

if __name__ == '__main__':

    l = [1,2,3]
    t = (1,2,3,[1,2,3])

    l[0] = 5
    # t[0] = 5  # TypeError

    del(l[1:]) # deletes items 1 onwards

    print(l)

    # del(t[0]) # TypeError - item deletion not supported

    t[3][0] = 5 # items in tuple may themselves be mutable
    print(t)

    l = [2,3,4,5,67]
    print(heapq.nlargest(1,l))
    h = heapq.heapify(l)
    print(h)