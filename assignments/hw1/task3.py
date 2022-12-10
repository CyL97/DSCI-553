import sys
import json
import time
from pyspark.sql import SparkSession

if __name__ == '__main__':
    review_path = sys.argv[1]
    business_path = sys.argv[2]
    ans_a_path = sys.argv[3]
    ans_b_path = sys.argv[4]
    #print(review_path)
    #print(business_path)
    #Initalize
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    ans = {}
    t_common_s = time.time() 
    lines_r = spark.read.json(review_path).rdd
    lines_b = spark.read.json(business_path).rdd
    
    #Q-A
    m_r = lines_r.map(lambda row: (row['business_id'], row['stars']))
    m_b = lines_b.map(lambda row: (row['business_id'], row['city']))
    sequenceF = (lambda x,y:(x[0] + y, x[1] + 1))
    combineF = (lambda x,y:(x[0] + y[0], x[1] + y[1]))
    lines_c = m_b.join(m_r).map(lambda row: row[1]).aggregateByKey((0, 0), sequenceF, combineF).map(lambda x: (x[0], x[1][0] / x[1][1]))
    t_common_e = time.time()
    t_common = t_common_e - t_common_s
    
    lines_c_out = lines_c.sortBy(lambda x: (-x[1], x[0])).collect()
    
    #print(lines_c)
    
    with open(ans_a_path, 'w') as f:
        line = 'city,stars\n'
        f.write(line)
        for line in lines_c_out:
            #print(line)
            line = str(line).strip('(').strip(')').replace('\'','').replace(', ',',')+'\n'
            f.write(line)
            
    #Q-B
    #Python
    '''tp_s = time.time()
    cities = {}
    for line in open(business_path,'r'):
        line_dict = json.loads(line)
        if 'city' in line_dict.keys():
            #print(line_dict)
            if line_dict['business_id'] not in cities.keys():
                cities[line_dict['business_id']] = []
            if line_dict['city'] not in cities[line_dict['business_id']]:
                cities[line_dict['business_id']].append(line_dict['city'])
    #print(cities)
    reviews = {}
    for line in open(review_path,'r'):
        line_dict = json.loads(line)
        if line_dict['business_id'] not in reviews.keys():
            reviews[line_dict['business_id']] = []
            reviews[line_dict['business_id']].append(float(line_dict['stars']))
        else:
            reviews[line_dict['business_id']].append(float(line_dict['stars']))
    #print(reviews)
    cities_star = {}
    for bus_id, city in cities.items():
        for bus_id_r, stars in reviews.items():
            if bus_id == bus_id_r:
                if city[0] not in cities_star:
                    cities_star[city[0]] = {}
                    cities_star[city[0]]['sum'] = sum(stars)
                    cities_star[city[0]]['len'] = len(stars)
                else:
                    cities_star[city[0]]['sum'] += sum(stars)
                    cities_star[city[0]]['len'] += len(stars)
    #print(cities_star)
    cities_rank = []
    for city, values in cities_star.items():
        cities_rank.append((city, values['sum'] / values['len']))
    cities_rank.sort(key=lambda x:(-x[1], x[0]))
    print(cities_rank[:10])
    tp_e = time.time()
    ans['m1'] = tp_e - tp_s'''

    tp_s = time.time()
    cities_rank = lines_c.collect()
    cities_rank.sort(key=lambda x:(-x[1], x[0]))
    tp_e = time.time()
    ans['m1'] = t_common + tp_e - tp_s
    
    #Spark
    ts_s = time.time()
    lines_s = lines_c.sortBy(lambda x: (-x[1], x[0])).take(10)
    ts_e = time.time()
    ans['m2'] = t_common + ts_e - ts_s
    print("Method1: ", cities_rank[:10],"\nMethod2: ", lines_s)
    ans['reason'] = 'For Method#2, I used "sortBy" function, which will involve a Shuffle function. The Shuffle function will assign each element in lines_c to a partition, so it will copy data across executors and machines, which costs a lot of time. But for Method#1, I used "sort" built-in function to sort a list from RDD. This fuction only sort all elements in one partition. Thus, Method#1 is much faster than Method#2.'
    #print(ans)
    
    with open(ans_b_path, 'w') as f:
        json.dump(ans, f)