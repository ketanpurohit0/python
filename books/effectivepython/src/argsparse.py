import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')

    parser.add_argument("-a", nargs='?', type=int)
    parser.add_argument("-b", nargs='?')
    parser.add_argument("-c", nargs='?', type=str)
    parser.add_argument("-d", nargs='?', type=json.loads)

    args = parser.parse_args()
    print(vars(args))
    asdict = vars(args)
    if asdict['d']:
        print(asdict["d"].get("K"))
        print(asdict["d"].get("Z"))

    # python .\src\argsparse.py -d "{\"K\": 3000, \"Z\": 0}"

    # args = parser.parse_args(["-a", "3", "-b", "2", "-c", '"3"'])
    # print(vars(args))
    # args = parser.parse_args(["-a", "3", "-b", "2"])
    # print(vars(args))
    # args = parser.parse_args(["-a", "3", "-b", "2"])
    # print(vars(args))
    #
    # args = parser.parse_args(["-d", '{"K": 3000, "Z": 0}'])
    # asdict = vars(args)
    # print(asdict["d"].get("K"))
