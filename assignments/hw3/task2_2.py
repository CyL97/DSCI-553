from pyspark import SparkContext
import json
import sys
import time
import numpy as np
from xgboost import XGBRegressor

if __name__ == '__main__':
    folder_path = sys.argv[1]
    val_path = sys.argv[2]
    output_path = sys.argv[3]
    
    s_t = time.time()
    
    spark = SparkContext(appName= "task2_2")
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
    
    #x: [[useful, funny, cool, user_avg_star, user_review_cnt, user_fans, bus_avg_star, bus_review_cnt]...]
    #y: rating
    
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
    
    result_str = "user_id, business_id, prediction\n"
    for i in range(0, len(Y_pred)):
        result_str += user_bus_list[i][0] + "," + user_bus_list[i][1] + "," + str(Y_pred[i]) + "\n"
    with open(output_path, "w") as f:
        f.writelines(result_str)
    
    e_t = time.time()
    print('Duration: ', e_t - s_t)

    #RMSE:  0.984658840861087 
    #{'<1': 101551, '1~2': 33417, '2~3': 6260, '3~4': 815, '4~5': 1}
    
    #Duration:  276.50632524490356                                                                                                                   #RMSE: 0.9821813409163227  