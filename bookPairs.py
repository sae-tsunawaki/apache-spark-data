# set up SparkContext for bookPairs application
from pyspark import SparkContext
import itertools
sc = SparkContext("local", "bookPairs")

# the main map-reduce task
lines = sc.textFile("/home/cs143/data/goodreads.dat")
words = lines.map(lambda line: line.split(":")[1].split(","))
w1 = words.flatMap(lambda x: itertools.combinations(x, 2)).map(lambda x: (int(x[0]), int(x[1])))
w2 = w1.filter(lambda x: x[0] != x[1])
w3 = w2.map(lambda pair: (pair, 1))
pairCounts = w3.reduceByKey(lambda a, b: a+b)
finalPair = pairCounts.filter(lambda x: x[1] > 20)
finalPair.saveAsTextFile("/home/cs143/output1")

