import binascii
from sys import argv
import random
import elly_func
import datetime
import csv
from statistics import mean


def hash_func(x, a, b, p):
    h_collect = []
    for i in range(len(a)):
        h = (a[i] * x + b[i]) % p
        binary = bin(h)
        h_collect.append([zeros_num(binary)])
        return h_collect


def g(x, y):
    return x + y


def zeros_num(x):
    i = 0
    xx = str(x)[:1:-1]
    for a in xx:
        if a != '0':
            return i
        else:
            i = i + 1
    else:
        # if there is no 0 in the end of binary
        s = 1
        return s


def make_list(x, y):
    k = list(map(g, x, y))
    return k


def FM(w, a, b, prime, output_file):
    t = str(datetime.datetime.now())[:-7]

    null = w.map(lambda x: hash_func(x, a, b, prime)).reduce(lambda x, y: make_list(x, y))
    distinct_candidate_num = w.distinct().count()
    R = mean(map(max, null))
    e = int(pow(2, R))

    with open(output_file, 'a') as fs:
        result = csv.writer(fs)
        result.writerow([t, distinct_candidate_num, e])


def main():
    port_num = int(argv[1])
    output_file = argv[2]
    window_box = 30
    window_sec = 10
    ssc = elly_func.streaming(elly_func.start_spark('task2'), 5)

    first_row = ['Time', 'Ground Truth', 'Estimation']
    with open(output_file, 'w') as fs:
        out = csv.writer(fs)
        out.writerow(first_row)

    # prime_num
    prime = 431
    # objects in the set
    m = 101
    # hash_num
    k = 20

    seeds = []
    for i in range(1, m * k):
        seeds.append(i)

    c = []
    for i in range(1, k * 600):
        c.append(i)

    a = random.sample(seeds, k)
    b = random.sample(c, k)

    stream = ssc.socketTextStream('localhost', port_num).map(elly_func.tojson).filter(
        lambda x: x != '').map(lambda x: int(binascii.hexlify(x.encode('utf8')), 16))
    w = stream.window(window_box, window_sec).foreachRDD(lambda x: FM(x, a, b, prime, output_file))

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()