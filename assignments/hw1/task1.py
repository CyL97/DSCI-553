import sys
import json
from pyspark.sql import SparkSession

if __name__ == '__main__':
    review_path = sys.argv[1]
    ans_path = sys.argv[2]
    
    #Initalize
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    ans = {}
    lines = spark.read.json(review_path).rdd
    
    #Q-A
    ans['n_review'] = lines.count()
    
    #Q-B
    ans['n_review_2018'] = lines.filter(lambda row: row['date'][:4] == '2018').count()
    
    #Q-C
    ans['n_user'] = lines.map(lambda row: row['user_id']).distinct().count()

    #Q-D
    #ans['top10_user'] = lines.map(lambda row: row['user_id']).countByValue()
    ans['top10_user'] = lines.map(lambda row: (row['user_id'], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: (-x[1], x[0])).map(lambda row: list(row)).take(10)

    #Q-E
    ans['n_business'] = lines.map(lambda row: row['business_id']).distinct().count()
    
    #Q-F
    ans['top10_business'] = lines.map(lambda row: (row['business_id'], 1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x: (-x[1], x[0])).map(lambda row: list(row)).take(10)

    #print(ans)

    with open(ans_path, 'w') as f:
        json.dump(ans, f)