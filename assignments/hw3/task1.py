from pyspark import SparkContext
import sys
import time
import random
from itertools import combinations
import operator

if __name__ == '__main__':
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    s_t = time.time()
    
    spark = SparkContext(appName= "task1")
    lines = spark.textFile(input_path)
    first = lines.first()
    lines = lines.filter(lambda row: row != first).map(lambda row: row.split(","))
    #print(raw_rdd.take(10))
    
    bus_user = lines.map(lambda row: (row[1], row[0])).groupByKey().mapValues(set)
    #print(bus_user.take(10))
    bus_user_dict = {}
    for bus, users in bus_user.collect():
        bus_user_dict[bus] = users
    users = lines.map(lambda row: row[0]).distinct()
    users_dict = {}
    i = 0
    for user in users.collect():
        users_dict[user] = i
        i += 1
        
    n = 60
    m = i
    p = 1e9 + 7
    hash_funcs = [] #[a, b]
    a = random.sample(range(1, m), n)
    hash_funcs.append(a)
    b = random.sample(range(1, m), n)
    hash_funcs.append(b)
    #print(hash_funcs)
    
    sign_dict = {}
    for bus, user_list in bus_user.collect():
        minhash_sign_list = []
        for i in range(n):
            minhash = float("inf")
            for user in user_list:
                minhash = min(minhash, (((hash_funcs[0][i] * users_dict[user] + hash_funcs[1][i]) % p) % m))
            minhash_sign_list.append(int(minhash))
        sign_dict[bus] = minhash_sign_list
    #print(sign)
    
    r = 2
    b = n // r
    bands_dict = {}
    for bus, minhash_sign in sign_dict.items():
        for i in range(0, b):
            #print(s[1][i*r: i*r+r])
            idx = (i, tuple(minhash_sign[i*r: i*r+r]))
            if idx not in bands_dict.keys():
                   bands_dict[idx] = []
                   bands_dict[idx].append(bus)
            else:
                   bands_dict[idx].append(bus)
    #print(bands_dict)
    bands_dict_fi = {}
    for key, values in bands_dict.items():
        if len(values) > 1:
            bands_dict_fi[key] = values
    #print(bands_dict_fi)
    #418426
    candidates = set()
    for values in bands_dict_fi.values():
        comb_list = combinations(sorted(values), 2)
        for item in comb_list:
            candidates.add(item)
    #print(candidates)
    
    result = {}
    for bus1, bus2 in candidates:
        user1 = bus_user_dict[bus1]
        user2 = bus_user_dict[bus2]
        js = len(user1 & user2) / len(user1 | user2)
        if js >= 0.5:
            result[str(bus1) + "," + str(bus2)] = js
    result = dict(sorted(result.items(), key=operator.itemgetter(0)))
    result_str = "business_id_1, business_id_2, similarity\n"
    for key, values in result.items():
        result_str += key + "," + str(values) + "\n"
    with open(output_path, "w") as f:
        f.writelines(result_str)

    e_t = time.time()
    print('Duration: ', e_t - s_t)