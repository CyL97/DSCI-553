from pyspark import SparkContext
import os
import sys
import time
import json
import numpy as np
from xgboost import XGBRegressor

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

def item_based_main(folder_path, val_path, output_path):
    spark = SparkContext(appName= "task2_1")
    #train data
    lines_train = spark.textFile(folder_path + '/yelp_train.csv')
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
    
    preds = []
    for row in lines_val.collect():
        prediction = item_based(row[0], row[1])
        preds.append(prediction)
    return preds

def model_based_main(train_path, val_path, output_path):
    #Train rating data
    lines_train = spark.textFile(folder_path + '/yelp_train.csv')
    first_train = lines_train.first()
    lines_train = lines_train.filter(lambda row: row != first_train).map(lambda row: row.split(","))

    #Get reviews att (busid, uesful, funny, cool)
    lines_review = spark.textFile(folder_path + '/review_train.json').map(lambda row: json.loads(row)).map(lambda row: (row['business_id'], (float(row['useful']), float(row['funny']), float(row['cool'])))).groupByKey().mapValues(list)
    review = {}
    for bus, items in lines_review.collect():
        review[bus] = items
    #useful_to = 0
    #funny_to = 0
    #cool_to = 0
    #num_to = 0
    review_dict = {}
    for keys, values in review.items():
        useful = 0
        funny = 0
        cool = 0
        num = 0
        for temp in values:
            temp_list = list(temp)
            useful += temp_list[0]
            funny += temp_list[1]
            cool += temp_list[2]
            num += 1
        #useful_to += useful
        #funny_to += funny
        #cool_to += cool
        #num_to += num
        review_dict[keys] = (useful / num, funny / num, cool / num)
    #print(useful_to, funny_to, cool_to)
    #print(useful_to / num_to, funny_to / num_to, cool_to / num_to)
    #print(review_dict)

    #Get user att (userid, avg_stars, review_count, fans)
    lines_user = spark.textFile(folder_path + '/user.json').map(lambda row: json.loads(row)).map(lambda row: (row['user_id'], (float(row['average_stars']), float(row['review_count']), float(row['fans']))))
    user_dict = {}
    for user, items in lines_user.collect():
        user_dict[user] = items
        
    #Get bus att (busid, stars, review_count)
    lines_bus = spark.textFile(folder_path + '/business.json').map(lambda row: json.loads(row)).map(lambda row: (row['business_id'], (float(row['stars']), float(row['review_count']))))
    bus_dict = {}
    for bus, items in lines_bus.collect():
        bus_dict[bus] = items

    #Train X and Y
    X_train = []
    Y_train = []
    for user, bus, rating in lines_train.collect():
        Y_train.append(rating)
        if bus in review_dict.keys():
            useful = review_dict[bus][0]
            funny = review_dict[bus][1]
            cool = review_dict[bus][2]
        else:
            useful = None 
            funny = None
            cool = None
        if user in user_dict.keys():
            user_avg_star = user_dict[user][0]
            user_review_cnt = user_dict[user][1]
            user_fans = user_dict[user][2]
        else:
            user_avg_star = None
            user_review_cnt = None
            user_fans = None
        if bus in bus_dict.keys():
            bus_avg_star = bus_dict[bus][0]
            bus_review_cnt = bus_dict[bus][1]
        else:
            bus_avg_star = None
            bus_review_cnt = None
        X_train.append([useful, funny, cool, user_avg_star, user_review_cnt, user_fans, bus_avg_star, bus_review_cnt])
    
    X_train = np.array(X_train, dtype='float32')
    Y_train = np.array(Y_train, dtype='float32')
    
    #Val X
    lines_val = spark.textFile(val_path)
    first_val = lines_val.first()
    lines_val = lines_val.filter(lambda row: row != first_val).map(lambda row: row.split(","))
    
    user_bus_list = []
    X_val = []
    #Y_val = []
    for lines in lines_val.collect():
        user = lines[0]
        bus = lines[1]
        #Y_val.append(lines[2])
        user_bus_list.append((user, bus))
        if bus in review_dict.keys():
            useful = review_dict[bus][0]
            funny = review_dict[bus][1]
            cool = review_dict[bus][2]
        else:
            useful = None 
            funny = None
            cool = None
        if user in user_dict.keys():
            user_avg_star = user_dict[user][0]
            user_review_cnt = user_dict[user][1]
            user_fans = user_dict[user][2]
        else:
            user_avg_star = None
            user_review_cnt = None
            user_fans = None
        if bus in bus_dict.keys():
            bus_avg_star = bus_dict[bus][0]
            bus_review_cnt = bus_dict[bus][1]
        else:
            bus_avg_star = None
            bus_review_cnt = None
        X_val.append([useful, funny, cool, user_avg_star, user_review_cnt, user_fans, bus_avg_star, bus_review_cnt])
    
    X_val = np.array(X_val, dtype='float32')
    #Y_val = np.array(Y_val, dtype='float32')
    param = {
        'lambda': 9.92724463758443, 
        'alpha': 0.2765119705933928, 
        'colsample_bytree': 0.5, 
        'subsample': 0.8, 
        'learning_rate': 0.02, 
        'max_depth': 17, 
        'random_state': 2020, 
        'min_child_weight': 101,
        'n_estimators': 300,
    }
    xgb = XGBRegressor(**param)
    xgb.fit(X_train, Y_train)
    Y_pred = xgb.predict(X_val)    
    return user_bus_list, Y_pred

if __name__ == '__main__':
    folder_path = sys.argv[1]
    val_path = sys.argv[2]
    output_path = sys.argv[3]
    
    s_t = time.time()
    
    spark = SparkContext(appName= "task2_3")
    spark.setLogLevel("ERROR")
    #train data
    lines_train = spark.textFile(folder_path + '/yelp_train.csv')
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
    
    item_based_result = []
    for row in lines_val.collect():
        prediction = item_based(row[0], row[1])
        item_based_result.append(prediction)
    
    user_bus_list, model_based_result = model_based_main(folder_path, val_path, output_path)
    #print(item_based_result)
    #print(model_based_result)
    
    #min_factor = 0
    #min_result = float('inf')
    #for factor in np.arange(0, 1, 0.01):
    factor = 0.1
    result_str = "user_id, business_id, prediction\n"
    for i in range(0, len(model_based_result)):
        result = float(factor) * float(item_based_result[i]) + (1 - float(factor)) * float(model_based_result[i])
        result_str += user_bus_list[i][0] + "," + user_bus_list[i][1] + "," + str(result) + "\n"
    with open(output_path, "w") as f:
        f.writelines(result_str)

    e_t = time.time()
    print('Duration: ', e_t - s_t)   
    
    # 0.05: RMSE: 0.9840299463998948
    # 0.1: RMSE: 0.9838090421925444 // RMSE: 0.9821889853088509
    # 0.15: RMSE: 0.983996402853830
    # 0.2: RMSE: 0.9845917953142862
    # 0.3: RMSE: 0.9870032159351917