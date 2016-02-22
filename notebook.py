
from pyspark import SparkContext

# SparkContext instance
sc = SparkContext('local[2]', 'pyspark')
print sc


# create RDDs from collections
data = range(1, 6)
print data

# no computation occurs here
# because Spark just simply records how to create the RDD
rangeRDD = sc.parallelize(data, 2)
print rangeRDD


# create RDDs from a file
txtFile = sc.textFile('test.txt', 3)
print txtFile


# map
timesthreeRDD = rangeRDD.map(lambda x: x * 3)
# collect is an action which triggers computation
print timesthreeRDD.collect()

# filter
evenRDD = rangeRDD.filter(lambda x: x % 2 == 0)
print evenRDD.collect()

# flatMap
selfandtimestwoRDD = rangeRDD.flatMap(lambda x: [x, x*2])
print selfandtimestwoRDD.collect()

# collect is shown above



# take
print rangeRDD.take(3)


# takeOrdered
print rangeRDD.takeOrdered(3, lambda x: -1 * x)


from operator import add
import re

def tokenize(text):
    # [^\w] matches anything that's not alphanumeric or underscore
    return re.sub(r'[^\w]', ' ', text).lower().split()

words = txtFile.flatMap(tokenize)

wcpairs = words.map(lambda x: (x,1))

counts = wcpairs.reduceByKey(add)
# print counts

# apply 'collect' action
wcresult = counts.collect()
# sort the result for output
resultsorted = sorted(wcresult, key=(lambda (w,c): c), reverse=True)

print resultsorted


from collections import defaultdict
import operator

rawFile = sc.textFile('users.txt')

def makePair(line):
    strFollowers = line.split(':')[1]
    followers = map(int, strFollowers.split(','))
    count = len(followers)
    pairs = []
    if count > 1:
        for index_i in range(count):
            for index_j in range(index_i+1, count):
                pairs.append((followers[index_i], followers[index_j]))
                pairs.append((followers[index_j], followers[index_i]))
    return pairs

def makeDict(record):
    """
    record: (active user, [users who share a same followee with the active user])
    """
    result = defaultdict(int)
    for user in record[1]:
        result[user] += 1
    return (record[0], sorted(result.iteritems(), key=operator.itemgetter(1), reverse=True))

pairsRDD = rawFile.flatMap(makePair)
print 'pairs: ', pairsRDD.collect()

groupRDD = pairsRDD.groupByKey()
# print groupRDD.collect()

dictRDD = groupRDD.map(makeDict)
print dictRDD.collect()



