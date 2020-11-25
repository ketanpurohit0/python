import src.power_sum as s


def test_is_a_whole_root_n() -> None:
    for n in range(0, 15):
        V = [x**n for x in range(15)]
        VV = [s.isWholeRootOfN(x, n) for x in V]
        assert(all(VV))
