from pyspark import SparkContext
from itertools import combinations as comb
import time
import sys
import copy
import random

def hash(x, y, n_bucket):
  return (x ^ y) % n_bucket

def f_partition(x):
    return (len(x[0]) + random.randint(0, 1000)) % int(n_partitions)

def group_prod(lines, k):
    return lines.map(lambda row: (row[0], row[1])).groupByKey().mapValues(set).map(lambda row: (row[0], list(row[1]))).filter(lambda row: len(row[1]) > k)

def PCY(baskets, total_n, support, n_bucket):
    baskets = list(baskets)
    item_cnt = {}
    hash_cnt = {}
    bitmap=[0 for x in range(0, n_bucket)]
    candidates = []
    p = len(baskets) / total_n
    t = p * support

    #PCY pass1
    for basket in baskets:
        for item in basket:
            if item not in item_cnt.keys():
                item_cnt[item] = 1
            else:
                item_cnt[item] += 1
        for i in range(0, len(basket) - 1):
            for j in range(i + 1, len(basket)):
                idx = hash(int(basket[i]), int(basket[j]), n_bucket)
                if idx not in hash_cnt.keys():
                    hash_cnt[idx] = 1
                else:
                    hash_cnt[idx] += 1
        for key, value in hash_cnt.items():
            if value >= t:
                bitmap[key] = 1
                
    #PCY pass2
    single_freq = []
    for key, value in item_cnt.items():
        if value >= t:
            single_freq.append(key)
            candidates.append((tuple([key]), 1))
    #single_freq = sorted(single_freq)
    #print(candidates)
    pair_freq = []
    for i in range(0, len(single_freq) - 1):
        for j in range(i + 1, len(single_freq)):
            pair_freq.append((single_freq[i], single_freq[j]))
    for pair in pair_freq:
        if bitmap[hash(int(pair[0]), int(pair[1]), n_bucket)] != 1:
            pair_freq.remove(pair)
            
    #print(candidates)
    if len(pair_freq) == 0:
        return []
    else:
        #pair_freq = sorted(pair_freq)
        #print(pair_freq)
        baskets_temp = []
        for basket in baskets:
            basket_1 = copy.deepcopy(basket)
            for item in basket:
                if item not in single_freq:
                    basket_1.remove(item)
            baskets_temp.append(basket_1)
        baskets = baskets_temp
        cnt_dict = dict(zip(pair_freq, [0] * len(pair_freq)))
        for basket in baskets:
            for key in cnt_dict.keys():
                if set(list(key)) <= set(basket):
                    cnt_dict[key] += 1
        for key, value in cnt_dict.copy().items():
            if value < t:
                del cnt_dict[key]
            else:
                candidates.append((tuple(key), 1))
        candidate = cnt_dict.keys()
        #print(candidate)
        k = 3
        while(True):
            #candidate = sorted(candidate)
            temp = list(comb(set(a for b in candidate for a in b), k))
            cnt_dict = dict(zip(temp, [0] * len(temp)))
            for basket in baskets:
                for key in cnt_dict.keys():
                    #print(set(list(key)), set(basket))
                    if set(list(key)) <= set(basket):
                        cnt_dict[key] += 1
            #print(cnt_dict, t)
            for key, value in cnt_dict.copy().items():
                if value < t:
                    del cnt_dict[key]
                else:
                    candidates.append((tuple(key), 1))
            candidate = cnt_dict.keys()
            #print(k, len(candidate))
            if len(candidate) > 0:
                k += 1
            else:
                break
        #print(candidates)
        return candidates
    
def SON(baskets, candidates):
    baskets = list(baskets)
    result = {}
    for basket in baskets:
        for items in candidates:
            flag = 0
            for item in items:
                if item not in basket:
                    flag = 1
                    break
            if flag == 0:
                idx = tuple(items)
                if idx not in result.keys():
                    result[idx] = 1
                else:
                    result[idx] += 1
    return result.items()

if __name__ == '__main__':
    filter_t = sys.argv[1]
    support = sys.argv[2]
    input_path = sys.argv[3]
    output_path = sys.argv[4]
    
    n_bucket = 1000
    
    t_s = time.time()
    spark = SparkContext(appName= "task2")
    t_s = time.time()
    #spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    #lines = spark.sparkContext.textFile(input_path)
    lines = spark.textFile(input_path)
    #print(lines.collect())
    lines_header = lines.first()
    lines = lines.filter(lambda row: row != lines_header).map(lambda row: row.split(',')).map(lambda row: [row[0][1:-5] + row[0][-3:-1] + '-' + str(int(row[1][1:-1])), str(int(row[5][1:-1]))])
    
    #Output preprocess result
    preprocess = "DATE-CUSTOMER_ID,PRODUCT_ID\n"
    lines_list = lines.collect()
    for row in lines_list:
        temp = str(row[0]) + ',' + str(row[1]) + '\n'
        preprocess += temp
    with open('./customer_product.csv', 'w+') as f:
        f.write(preprocess)
    
    baskets = group_prod(lines, int(filter_t))
    
    #SON algorithm stage #1 -> PCY
    n_partitions = 10
    total_n = baskets.count()
    baskets = baskets.partitionBy(n_partitions, f_partition)
    p = baskets.getNumPartitions()
    items = baskets.values()
    a = items.collect()
    #print(p)
    candidates = items.mapPartitions(lambda baskets: PCY(baskets, total_n, float(support), n_bucket)).reduceByKey(lambda x, y: x + y).collect()
    #print(candidates)
    result_temp = []
    for i in range(0, len(candidates)):
        result_temp.append(sorted(candidates[i][0]))
    result_temp = sorted(result_temp)
    result_temp = sorted(result_temp, key = lambda x: len(x))
    last = result_temp[-1]
    for i in range(len(result_temp) - 2, -1, -1):
        if last == result_temp[i]:
            del result_temp[i]
        else:
            last = result_temp[i]
    #print(result_temp)
    
    #Output candidates
    result = 'Candidates:\n'
    l = 1
    for i in range(0, len(result_temp)):
        if len(result_temp[i]) == 1:
            result = result + '(' + str(result_temp[i])[1:-1] + '),'
        else:
            if len(result_temp[i]) != l:
                result = result[:-1] + "\n\n" + '(' + str(result_temp[i])[1:-1] + "),"
                l = len(result_temp[i])
            else:
                result = result + '(' + str(result_temp[i])[1:-1] + "),"
    result = result[:-1] + '\n\n'
    #print(result)
    
    #SON algorithm stage #2
    freq_items = items.mapPartitions(lambda baskets: SON(baskets, result_temp)).reduceByKey(lambda x, y: x + y).filter(lambda x : x[1] >= int(support)).collect()
    result_temp = []
    for i in range(0, len(freq_items)):
        result_temp.append(sorted(freq_items[i][0]))
    result_temp = sorted(result_temp)
    result_temp = sorted(result_temp, key = lambda x: len(x))
    last = result_temp[-1]
    for i in range(len(result_temp) - 2, -1, -1):
        if last == result_temp[i]:
            del result_temp[i]
        else:
            last = result_temp[i]
    #print(result_temp)
    result += 'Frequent Itemsets:\n'
    l = 1
    for i in range(0, len(result_temp)):
        if len(result_temp[i]) == 1:
            result = result + '(' + str(result_temp[i])[1:-1] + '),'
        else:
            if len(result_temp[i]) != l:
                result = result[:-1] + "\n\n" + '(' + str(result_temp[i])[1:-1] + "),"
                l = len(result_temp[i])
            else:
                result = result + '(' + str(result_temp[i])[1:-1] + "),"
    result = result[:-1]
    with open(output_path, 'w+') as f:
        f.write(result)
        
    t_e = time.time()
    #print(result_t)
    print("Duration: ", t_e - t_s)
    #print(filter_t, support)
    #print(lines.collect())