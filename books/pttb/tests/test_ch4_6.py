from collections import namedtuple

import pytest

CarFactory = namedtuple('Car', ['color', 'mileage'])
ElectricCarFactory = namedtuple('ElectricCar', CarFactory._fields + ('charge',))
HybridCarFactory = namedtuple('Hybrid', ElectricCarFactory._fields + ('range',))



def test_1():
    car = CarFactory('red', 3000)
    # print(f"{car!r}")
    assert(car.color == 'red')
    assert(car[0] == 'red')
    color, _ = car
    assert(color == 'red')
    with pytest.raises(AttributeError):
        car.color = 'blue'


def test_extend_car():
    elec = ElectricCarFactory('red', 3000, '240V')
    assert(elec.charge == '240V')
    c = list(elec._fields).append('Torque')


def test_extend_car_to_hybrid():
    hybrid = HybridCarFactory('red',3000, '180V', 400)
    assert(hybrid.range == 400)
    dict = hybrid._asdict()
    assert (hybrid._replace(color='blue').color == 'blue')
    hybrid2 = HybridCarFactory._make(['red', 3000, '180V', 900])
    assert(hybrid2.color == 'red' and hybrid2.range == 900)