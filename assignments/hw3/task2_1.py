from pyspark import SparkContext
import os
import sys
import time

def item_based(bus, user):
    if user not in user_bus_dict.keys():
        return 3.5
    if bus not in bus_user_dict.keys():
        return user_avg_dict[user]
    
    w_list = []

    for bus1 in user_bus_dict[user]:
        temp = tuple(sorted((bus1, bus)))
        if temp in w_dict.keys():
            w = w_dict[temp]
        else:
            #co-rated
            user_inter = bus_user_dict[bus] & bus_user_dict[bus1]

            if len(user_inter) <= 1:
                w = (5.0 - abs(bus_avg_dict[bus] - bus_avg_dict[bus1])) / 5
            elif len(user_inter) == 2:
                user_inter = list(user_inter)
                w1 = (5.0 - abs(float(bus_user_r_dict[bus][user_inter[0]]) - float(bus_user_r_dict[bus1][user_inter[0]]))) / 5 
                w2 = (5.0 - abs(float(bus_user_r_dict[bus][user_inter[1]]) - float(bus_user_r_dict[bus1][user_inter[1]]))) / 5
                w = (w1 + w2) / 2
            else:
                r1 = []
                r2 = []
                for user1 in user_inter:
                    r1.append(float(bus_user_r_dict[bus][user1]))
                    r2.append(float(bus_user_r_dict[bus1][user1]))
                avg1 = sum(r1) / len(r1)
                avg2 = sum(r2) / len(r2)
                temp1 = [x - avg1 for x in r1]
                temp2 = [x - avg2 for x in r2]
                X = (sum([x * y for x,y in zip(temp1, temp2)]))
                Y = ((sum([x ** 2 for x in temp1])**(1/2)) * (sum([x ** 2 for x in temp2])**(1/2)))
                if Y == 0:
                    w = 0
                else:
                    w = X / Y 
            w_dict[temp] = w
        w_list.append((w, float(bus_user_r_dict[bus1][user])))      
    w_list_can = sorted(w_list, key=lambda x: -x[0])[:15]
    X = 0
    Y = 0
    for w, r in w_list_can:
        X += (w * r)
        Y += abs(w)
    if Y == 0:
        return 3.5
    else:
        return X / Y

if __name__ == '__main__':
    train_path = sys.argv[1]
    val_path = sys.argv[2]
    output_path = sys.argv[3]
    
    s_t = time.time()
    
    spark = SparkContext(appName= "task2_1")
    #train data
    lines_train = spark.textFile(train_path)
    first_train = lines_train.first()
    lines_train = lines_train.filter(lambda row: row != first_train).map(lambda row: row.split(",")).map(lambda row: (row[1], row[0], row[2]))
    
    bus_user_train = lines_train.map(lambda row: (row[0], row[1])).groupByKey().mapValues(set)
    bus_user_dict = {}
    for bus, users in bus_user_train.collect():
        bus_user_dict[bus] = users
        
    user_bus_train = lines_train.map(lambda row: (row[1], row[0])).groupByKey().mapValues(set)
    user_bus_dict = {}
    for user, bus in user_bus_train.collect():
        user_bus_dict[user] = bus
    
    #bus_mid = lines_train.map(lambda row: (row[0], float(row[2]))).groupByKey().mapValues(list).map(lambda x: (x[0], sorted(x[1]))).map(lambda x: (x[0], x[1][len(x[1]) // 2]))
    bus_avg = lines_train.map(lambda row: (row[0], float(row[2]))).groupByKey().mapValues(list).map(lambda x: (x[0], sum(x[1]) / len(x[1])))
    bus_avg_dict = {}
    for bus, rating in bus_avg.collect():
        bus_avg_dict[bus] = rating

    #user_mid = lines_train.map(lambda row: (row[1], float(row[2]))).groupByKey().mapValues(list).map(lambda x: (x[0], sorted(x[1]))).map(lambda x: (x[0], x[1][len(x[1]) // 2]))
    user_avg = lines_train.map(lambda row: (row[1], float(row[2]))).groupByKey().mapValues(list).map(lambda x: (x[0], sum(x[1]) / len(x[1])))
    user_avg_dict = {}
    for user, rating in user_avg.collect():
        user_avg_dict[user] = rating

    bus_user_r = lines_train.map(lambda row: (row[0], (row[1], row[2]))).groupByKey().mapValues(set)
    bus_user_r_dict = {}
    for bus, user_r_set in bus_user_r.collect():
        temp = {}
        for user_r in user_r_set:
            temp[user_r[0]] = user_r[1]
        bus_user_r_dict[bus] = temp
    #print(bus_user_r_dict)
    
    #val data
    lines_val = spark.textFile(val_path)
    first_val = lines_val.first()
    lines_val = lines_val.filter(lambda row: row != first_val).map(lambda row: row.split(",")).map(lambda row: (row[1], row[0]))
    # (bus1, bus2): {simi}
    w_dict = {}
    
    result_str = "user_id, business_id, prediction\n"
    for row in lines_val.collect():
        prediction = item_based(row[0], row[1])
        result_str += row[1] + "," + row[0] + "," + str(prediction) + "\n"
    with open(output_path, "w") as f:
        f.writelines(result_str)
    
    e_t = time.time()
    print('Duration: ', e_t - s_t)
    
    
    #RMSE: 1.0475857031155809 