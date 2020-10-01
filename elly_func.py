import json
import sys
import csv

def streaming(sc, reload: int = 5):
    from pyspark.streaming import StreamingContext
    ssc = StreamingContext(sc, reload)
    return ssc

def start_spark(name = 'SparkTask', local = 'local[*]'):
    from pyspark import SparkContext
    from pyspark import SparkConf

    confSpark = SparkConf().setAppName(name).setMaster(
        local).set('spark.driver.memory', '4G').set(
            'spark.executor.memory', '4G')
    sc = SparkContext.getOrCreate(confSpark)
    sc.setLogLevel(logLevel='ERROR')
    return sc


def tojson(line):
    lines = line.splitlines()
    data = json.loads(lines[0])['city']
    return data


def form(hash_bus2):
    k = ' '.join(map(str, hash_bus2))
    return k


def check(common, x):
    if x is None:
        return 0
    else:
        for i in x:
            if i in common:
                return 1
            else:
                return 0


