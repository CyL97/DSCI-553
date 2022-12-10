from pyspark import SparkContext
import os
import sys
from itertools import combinations
import time
from pyspark.sql import *
from graphframes import *


os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell"

if __name__ == '__main__':
    sc = SparkContext(appName= "task1")
    sc.setLogLevel('ERROR')

    filter_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    output_file_path = sys.argv[3]

    task1_start = time.time()
    csvRDD = sc.textFile(input_file_path)
    head=csvRDD.first()
    inputRDD=csvRDD.filter(lambda x: x != head).map(lambda x: x.split(","))

    user = inputRDD.map(lambda x:x[0]).distinct()
    user_total = len(user.collect())

    business = inputRDD.map(lambda x:x[1]).distinct()
    business_total = len(business.collect())
    # user1: set{[b1,b2,b3]} 
    user_bus_dict= inputRDD.map(lambda row : (row[0], row[1])).groupByKey().map(lambda x:(x[0],sorted(list(x[1])))).mapValues(set).collectAsMap()

    # compare two users:
    user_pair_list = list(combinations(user.collect(),2))
    
    nodes = set()
    edges = list()
    for pair in user_pair_list:
       if len(user_bus_dict[pair[0]] & user_bus_dict[pair[1]])>= filter_threshold:
            nodes.add(pair[0])
            nodes.add(pair[1])
            edges.append((pair[0], pair[1]))
            edges.append((pair[1], pair[0]))
    nodes = list(nodes)
    #spark dataframe part:
    SQLContext = SQLContext(sc)
    
    nodes_dataframe = SQLContext.createDataFrame([tuple([vertices]) for vertices in nodes], ["id"])
    edges_dataframe = SQLContext.createDataFrame(edges, ["src", "dst"])
    
    GF = GraphFrame(nodes_dataframe, edges_dataframe)
    Community = GF.labelPropagation(maxIter=5)
    Community_result = Community.rdd.map(lambda x: (x[1],x[0])).groupByKey().map(lambda x: sorted(list(x[1])))\
    .sortBy(lambda x:(x[0])).sortBy(lambda x : (len(x)))
    


    with open(output_file_path,'w') as f:
        for i in Community_result.collect():
            f.write(str(i)[1:-1]+"\n")
    f.close()



    task1_end = time.time()
    task1_time = task1_end-task1_start
    print("Duration: ",task1_time)