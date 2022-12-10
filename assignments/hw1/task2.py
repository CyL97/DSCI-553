import sys
import json
import time
from pyspark.sql import SparkSession
import random

def f_partition(x):
    return (len(x[0]) + random.randint(0, 1000)) % int(n_partition)

def f(x):
    x_list = list(x)
    yield len(x_list)

if __name__ == '__main__':
    review_path = sys.argv[1]
    ans_path = sys.argv[2]
    n_partition = sys.argv[3]
    #print("n_partition", n_partition)
    #Initalize
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    ans = {}
    lines = spark.read.json(review_path).rdd
    
    #Default
    td_s = time.time()
    temp = lines.map(lambda row: (row['business_id'], 1))
    default = {}
    default['n_partition'] = temp.getNumPartitions()
    default['n_items'] = temp.mapPartitions(f).collect()
    d_temp = temp.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False).map(lambda row: [row[0], row[1]]).take(10)
    td_e = time.time()
    default['exe_time'] = td_e - td_s
    
    #Customized
    tc_s = time.time()
    temp = lines.map(lambda row: (row['business_id'], 1))
    temp = temp.partitionBy(int(n_partition), f_partition)
    customized = {}
    customized['n_partition'] = temp.getNumPartitions()
    customized['n_items'] = temp.mapPartitions(f).collect()
    c_temp = temp.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False).map(lambda row: [row[0], row[1]]).take(10)
    tc_e = time.time()
    customized['exe_time'] = tc_e - tc_s
    
    ans['default'] = default
    ans['customized'] = customized
    #print(ans)
    
    with open(ans_path, 'w') as f:
        json.dump(ans, f)