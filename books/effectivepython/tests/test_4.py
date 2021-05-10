def test_1():
    a = 0b10111011
    b = 0xc5f
    msg = 'b=%d h=%d' % (a, b)
    assert (msg == 'b=187 h=3167')


def test_2():
    kv = {'name': 'Ketan', 'address': 'London'}
    msg = 'name=%(name)s address=%(address)s' % kv
    assert(msg == 'name=Ketan address=London')


def test_format():
    a = 1234.5678
    r = format(a, ',.2f')
    assert(r == '1,234.57')
    v1 = 'a'
    v2 = 'b'
    template = '{} = {}'.format(v1, v2)
    assert(template == 'a = b')

    template = '{1} = {0}'.format(v1, v2)
    assert(template == 'b = a')

    d = {'k': 'v'}
    template = 'k={k}'.format(**d) # k = 'v'
    assert(template == 'k=v')