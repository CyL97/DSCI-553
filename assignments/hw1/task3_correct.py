import json
import sys
import glob
import shutil
import time
from pyspark import SparkContext
import os
from operator import add


os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

review_filepath = sys.argv[1]
business_filepath = sys.argv[2]
output_filepath_question_a = sys.argv[3]
output_filepath_question_b = sys.argv[4]

results = {}
sc = SparkContext('local[*]', 'task3')

reviewRDD = sc.textFile(review_filepath).map(json.loads).map(lambda s: (s['business_id'], s['stars']))
businessRDD = sc.textFile(business_filepath).map(json.loads).map(lambda s: (s['business_id'], s['city']))

# print(RDD.toDebugString().decode('utf-8'))
'''A. What are the average stars for each city? (1 point)'''
def seqFunc(U: (int, int), V: (float, int)):
    return U[0] + V[0], U[1] + V[1]
def combFunc(U1: (float, int), U2: (float, int)):
    return U1[0] + U2[0], U1[1] + U2[1]
RDD = businessRDD.join(reviewRDD).map(lambda x: (x[1][0], (x[1][1], 1)))\
    .aggregateByKey((0, 0), seqFunc, combFunc)\
    .map(lambda x: (x[0], x[1][0] / x[1][1]))
sortedRDD = RDD.sortBy(lambda x: (-x[1], x[0]))
# print(sortedRDD.collect())

output_file_a = open(output_filepath_question_a, 'w')
output_file_a.write('city,stars\n')
for line in sortedRDD.collect():
    output_file_a.write("%s,%s\n" % (line[0], line[1]))
output_file_a.close()

# using spark
# sortedRDD.map(lambda x: "%s,%s" % (x[0], x[1])).coalesce(1).saveAsTextFile("temp")
# with open(glob.glob('temp/part-00000*')[0], "r") as temptxt:
#     with open(output_filepath_question_a, 'w') as output_file_a:
#         output_file_a.write('city,stars\n')
#         for line in temptxt:
#             output_file_a.write(line)
# output_file_a.close()
# temptxt.close()
# shutil.rmtree("temp")


'''B. You are required to compare the execution time of using two methods to print top 10 cities with highest stars. '''
results = {}

begin = time.time()
m1 = RDD.collect()
m1.sort(key=lambda x: (-x[1], x[0]))
print(m1[0:10])
end = time.time()
results['m1'] = end - begin

begin = time.time()
m2 = RDD.sortBy(lambda x: (-x[1], x[0])).take(10)
print(m2)
end = time.time()
results['m2'] = end - begin
results['reason'] = "Sort data in spark is a type of transformation which triggers shuffling, this is a complex and " \
                    "expensive operation "

output_file_b = open(output_filepath_question_b, 'w')
json.dump(results, output_file_b, indent=4)
output_file_b.close()
