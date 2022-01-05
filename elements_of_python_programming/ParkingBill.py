

def solution(e, l):

    entrance_charge = 2
    first_hour_charge = 3
    subsquent_hour_charge = 4

    lseg = list(map(lambda x: int(x), l.split(":")))
    eseg = list(map(lambda x: int(x), e.split(":")))

    whole_hours = lseg[0] - eseg[0]
    whole_mins = lseg[1] - eseg[1]

    charge = first_hour_charge
    if whole_hours > 0:
        charge += (whole_hours-1)*subsquent_hour_charge

    if whole_hours > 0 and whole_mins > 0:
        charge += subsquent_hour_charge

    charge += entrance_charge

    return  charge

if __name__ == '__main__':

    es= ["10:00", "09:42", "10:00"]
    ls= ["13:21", "11:42", "10:30"]

    for e,l in zip(es, ls):
        print(solution(e,l))