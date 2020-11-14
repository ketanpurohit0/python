import src.fancy_sequence as f


def test_fancy_1():
    fancy: f.Fancy = f.Fancy()
    fancy.append(2)  # fancy sequence: [2]
    fancy.addAll(3)  # fancy sequence: [2+3] -> [5]
    fancy.append(7)  # fancy sequence: [5, 7]
    fancy.multAll(2)  # fancy sequence: [5*2, 7*2] -> [10, 14]
    assert(10 == fancy.getIndex(0))  # return 10
    fancy.addAll(3)  # fancy sequence: [10+3, 14+3] -> [13, 17]
    fancy.append(10)  # fancy sequence: [13, 17, 10]
    fancy.multAll(2)  # fancy sequence: [13*2, 17*2, 10*2] -> [26, 34, 20]
    assert(26 == fancy.getIndex(0))  # return 26
    assert(34 == fancy.getIndex(1))  # return 34
    assert(20 == fancy.getIndex(2))  # return 20


def test_fancy_2():
    fancy: f.Fancy = f.Fancy()
    fancy.append(5)  # fancy sequence: [2]
    fancy.addAll(5)  # fancy sequence: [2+3] -> [5]
    fancy.append(5)  # fancy sequence: [5, 7]
    fancy.multAll(5)  # fancy sequence: [5*2, 7*2] -> [10, 14]
    assert(50 == fancy.getIndex(0))  # return 10
    fancy.addAll(5)  # fancy sequence: [10+3, 14+3] -> [13, 17]
    fancy.append(5)  # fancy sequence: [13, 17, 10]
    fancy.multAll(5)  # fancy sequence: [13*2, 17*2, 10*2] -> [26, 34, 20]
    assert(275 == fancy.getIndex(0))  # return 26
    assert(-1 == fancy.getIndex(5))  # return 34
    assert(-1 == fancy.getIndex(5))  # return 20


def test_fancy_3():
    fancy: f.Fancy = f.Fancy()
    fancy.append(3)  # fancy sequence: [2]
    fancy.addAll(4)  # fancy sequence: [2+3] -> [5]
    fancy.append(7)  # fancy sequence: [5, 7]
    fancy.multAll(7)  # fancy sequence: [5*2, 7*2] -> [10, 14]
    fancy.append(3)
    fancy.addAll(4)
    assert(53 == fancy.getIndex(0))


def test_fancy_4():
    fancy: f.Fancy = f.Fancy()
    fancy.addAll(1)
    assert(-1 == fancy.getIndex(0))


def test_fancy_5():
    fancy: f.Fancy = f.Fancy()
    fancy.append(2)
    fancy.multAll(10)
    fancy.append(8)
    fancy.getIndex(0)
    fancy.addAll(9)
    fancy.append(8)
    fancy.append(10)
    fancy.getIndex(0)
    fancy.append(10)
    fancy.append(3)
    fancy.addAll(6)
    fancy.addAll(3)
    fancy.getIndex(3)
    fancy.append(5)
    fancy.getIndex(4)
    fancy.getIndex(0)
    fancy.addAll(8)
    fancy.getIndex(3)
    fancy.addAll(6)
    fancy.getIndex(7)
    fancy.getIndex(4)
    fancy.getIndex(3)
    fancy.append(1)
    fancy.addAll(8)
    fancy.append(5)
    fancy.getIndex(3)
    fancy.multAll(3)
    fancy.append(7)
    fancy.append(5)
    fancy.addAll(2)
    fancy.getIndex(3)
    fancy.addAll(2)
    fancy.getIndex(9)
