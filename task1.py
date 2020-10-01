import json
import binascii
import random
import sys
import csv
import math
import elly_func

def hash_func(x, m, a, b, k):
    # int: 18965941658415737
    if x is None:
        return None
    else:
        h_collect = set()
        for i in range(k):
            h = (a[i] * x + b[i]) % m
            h_collect.add(h)
        return h_collect

def main():
    business_first = argv[1] #'business_first.json'
    business_second = argv[2] # 'business_second.json'
    output_file = argv[3] #'task1.csv'

    # 860
    sc = elly_func.start_spark('bfr')
    rddtext = sc.textFile(business_first).map(elly_func.tojson).distinct().filter(
        lambda x: x != '').map(lambda x: int(binascii.hexlify(x.encode('utf8')), 16))

    # bits in array
    n = 100000
    # objects in the set
    m = rddtext.count()
    # hash_num
    k = 20
    f = 1 - math.e ** (-k * m / n)
    actual_rate = (f) ** k
    hash_num = m * k

    seeds = []
    for i in range(1, hash_num):
        seeds.append(i)

    c = []
    for i in range(1, k * 500):
        c.append(i)

    a = random.sample(seeds, k)
    b = random.sample(c, k)

    bloom = rddtext.map(lambda x: hash_func(x, n, a, b, k))
    common = bloom.reduce(lambda x, y: x.union(y))
    rddtext2 = sc.textFile(business_second).map(elly_func.tojson).map(
        lambda line: line if line != '' else None).map(
        lambda x: None if x is None else int(binascii.hexlify(x.encode('utf8')), 16))
    hash_bus2 = rddtext2.map(lambda x: hash_func(x, n, a, b, k)).map(
        lambda x: elly_func.check(common, x)).collect()
    result = elly_func.form(hash_bus2)

    with open(output_file, 'w') as fs:
        fs.write(result)

if __name__ == "__main__":
    main()