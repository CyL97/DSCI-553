#Method Description:
#The method I finally submitted has three steps: preprocessing, training and fine-tuning, and testing. In the preprocessing stage, after many experiments, I finally only selected the data in user.json and business.json. I first embed the entire dataset in a graph. Here I choose city, flatted category, user, and business as the points in the graph. I use the HIN2Vec model for graph embedding because this is a heterogeneous graph with millions of points. For heterogeneous graphs, HIN2Vec has better adaptability than traditional algorithms such as LINE or Node2Vec. I merged the embedding features generated by HIN2Vec with other common features of users and businesses. For categorical features, I use LabelEncoder for encoding; I normalize numerical features.
#HIN2Vec paper: https://dl.acm.org/doi/10.1145/3132847.3132953
#In the training phase, I use Xgbregressor to train all the integrated features and use the Optuna library to further fine-tune the model to get the best hyperparameters. After getting the optimal hyperparameters, I used 10-fold to cross-validate the training set, and the final predicted value will take the average of the cross-validation. I trained a total of four different models to deal with the cold start problem of user and business on the embedding dataset, user dataset, and business dataset.
#In the test phase, I combined the train and validation data sets for training so that the model can further learn the data characteristics on the validation set. This is ok in this project because we will eventually test on the test dataset. This is also my last committed version.

#Error Distribution:
#>=0 and <=1: 110936
#>=1 and <=2: 27051 
#>=2 and <=3: 3872
#>=3 and <=4: 185
#>=4: 0

#Train on train set, test on Val set: RMSE: 0.9715374647465116
#Train on train and val set, test on Val set: RMSE: 0.8565135387946218 

#Execution Time:
#Preprocessing: 38-42 hours on one 3039Ti
#Training & Fine-tune: 23-25 hours on one 3090Ti
#Testing: 907s

from pyspark import SparkContext
import time
import pandas as pd
import json
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import LabelEncoder, MinMaxScaler, OrdinalEncoder 
import pickle
import numpy as np
import sys

if __name__ == '__main__':
    folder_path = sys.argv[1]
    val_path = sys.argv[2]
    output_path = sys.argv[3]

    s_t = time.time()
    
    df_business = pd.read_csv('./business_name.csv')
    df_user = pd.read_csv('./user_name.csv')
    
    #df_train = pd.read_csv(folder_path + 'yelp_train.csv')
    business_features = pd.read_csv('./business.csv')
    user_features = pd.read_csv('./user.csv')
    embeddings = pd.read_csv('./emb_comb.csv')
    df_val = pd.read_csv(val_path)
    try:
        df_val.drop(['stars'], axis=1, inplace=True)
    except:
        pass
    val_user = set(df_val['user_id'].tolist())
    val_business = set(df_val['business_id'].tolist())
    #business_list = set(df_train['business_id'].tolist())
    #user_list = set(df_train['user_id'].tolist()[:len(df_train['user_id'].tolist())//10])
    business_list = set(df_business['business_id'].tolist())
    user_list = set(df_user['user_id'].tolist())
    
    
    comb = []
    users = []
    businesses = []
    nulls = []
    user_null = []
    business_null = []
    null = []
    comb_list = embeddings['id'].tolist()
    #comb_list = []
    for i in range(len(df_val)):
        business_id = df_val['business_id'][i]
        user_id = df_val['user_id'][i]
        if (user_id in comb_list) and (business_id in comb_list):
            comb.append(i)
        elif (user_id in comb_list) and (business_id not in comb_list) and (business_id in business_list):
            users.append(i)
        elif (user_id not in comb_list) and (business_id in comb_list) and (user_id in user_list):
            businesses.append(i)
        elif (user_id not in comb_list) and (business_id not in comb_list) and (user_id in user_list) and (business_id in business_list):
            nulls.append(i)
        elif (user_id not in comb_list) and (business_id not in comb_list) and (user_id not in user_list) and (business_id in business_list):
            business_null.append(i)
        elif (user_id not in comb_list) and (business_id not in comb_list) and (user_id in user_list) and (business_id not in business_list):
            user_null.append(i)
        else:
            null.append(i)
    print((len(comb), len(users), len(businesses), len(nulls), len(user_null), len(business_null), len(null)))
    df_comb = pd.DataFrame(df_val.iloc[comb])
    df_users = pd.DataFrame(df_val.iloc[users])
    df_businesses = pd.DataFrame(df_val.iloc[businesses])
    df_nulls = pd.DataFrame(df_val.iloc[nulls])
    df_user_null = pd.DataFrame(df_val.iloc[user_null])
    df_business_null = pd.DataFrame(df_val.iloc[business_null])
    df_null = pd.DataFrame(df_val.iloc[null])
    
    temp_user_list = []
    for i in df_user_null['user_id'].keys():
        temp_user_list.append(df_user[df_user['user_id'] == df_user_null['user_id'][i]]['average_stars'].values[0])
    df_user_null.insert(df_user_null.shape[1], 'pred', temp_user_list)
    
    temp_business_list = []
    for i in df_business_null['business_id'].keys():
        temp_business_list.append(df_business[df_business['business_id'] == df_business_null['business_id'][i]]['stars'].values[0])
    df_business_null.insert(df_business_null.shape[1], 'pred', temp_business_list)
    
    df_null.insert(df_null.shape[1], 'pred', 3.75)
    
    del df_business
    del df_user
    dense = ['stars', 'review_count_b', 'review_count_u', 'useful', 'funny', 'cool', 'fans', 
               'average_stars', 'compliment_hot', 'compliment_more', 'compliment_profile', 'compliment_cute', 
               'compliment_list', 'compliment_note', 'compliment_plain', 'compliment_cool', 'compliment_funny', 
               'compliment_writer', 'compliment_photos']
    
    comb_data = df_comb.merge(embeddings, left_on='business_id', right_on='id', how='left').set_axis(df_comb.index)
    comb_data = comb_data.merge(embeddings, left_on='user_id', right_on='id', how='left', suffixes=('_bus', '_user')).set_axis(comb_data.index)
    comb_data = comb_data.merge(business_features, on='business_id', how='left').set_axis(comb_data.index)
    comb_data = comb_data.merge(user_features, on='user_id', how='left').set_axis(comb_data.index)
    comb_data.drop(['user_id', 'business_id', 'id_bus', 'id_user'], axis=1, inplace=True)

    if len(comb_data) > 0:
        mms = MinMaxScaler(feature_range=(0,1))
        comb_data[dense] = mms.fit_transform(comb_data[dense])
    
    comb_res = []
    if len(comb_data) > 0:
        for i in range(10):
            model = pickle.load(open("./comb_ckpt/model_" + str(i) + "_comb.pkl", 'rb'))
            comb_res.append(list(model.predict(comb_data)))
    comb_res = np.array(comb_res)
    y_pred = np.mean(comb_res, axis=0)
    df_comb.insert(df_comb.shape[1], 'pred', y_pred)
    
    users_data = df_users.merge(embeddings, left_on='user_id', right_on='id', how='left').set_axis(df_users.index)
    users_data = users_data.merge(business_features, on='business_id', how='left').set_axis(users_data.index)
    users_data = users_data.merge(user_features, on='user_id', how='left').set_axis(users_data.index)
    users_data.drop(['user_id', 'business_id', 'id'], axis=1, inplace=True)

    if len(users_data) > 0:
        mms = MinMaxScaler(feature_range=(0,1))
        users_data[dense] = mms.fit_transform(users_data[dense])
        
    users_res = []
    if len(users_data) > 0:
        for i in range(10):
            model = pickle.load(open("./comb_ckpt/model_" + str(i) + "_comb_user.pkl", 'rb'))
            users_res.append(list(model.predict(users_data)))
    users_res = np.array(users_res)
    y_pred = np.mean(users_res, axis=0)
    df_users.insert(df_users.shape[1], 'pred', y_pred)
    
    businesses_data = df_businesses.merge(embeddings, left_on='business_id', right_on='id', how='left').set_axis(df_businesses.index)
    businesses_data = businesses_data.merge(business_features, on='business_id', how='left').set_axis(businesses_data.index)
    businesses_data = businesses_data.merge(user_features, on='user_id', how='left').set_axis(businesses_data.index)
    businesses_data.drop(['user_id', 'business_id', 'id'], axis=1, inplace=True)
    
    if len(businesses_data) > 0:
        mms = MinMaxScaler(feature_range=(0,1))
        businesses_data[dense] = mms.fit_transform(businesses_data[dense])
    businesses_res = []
    if len(businesses_data) > 0:
        for i in range(10):
            model = pickle.load(open("./comb_ckpt/model_" + str(i) + "_comb_business.pkl", 'rb'))
            businesses_res.append(list(model.predict(businesses_data)))
    businesses_res = np.array(businesses_res)
    y_pred = np.mean(businesses_res, axis=0)
    df_businesses.insert(df_businesses.shape[1], 'pred', y_pred)
    
    nulls_data = df_nulls.merge(business_features, on='business_id', how='left').set_axis(df_nulls.index)
    nulls_data = nulls_data.merge(user_features, on='user_id', how='left').set_axis(nulls_data.index)
    nulls_data.drop(['user_id', 'business_id'], axis=1, inplace=True)

    if len(nulls_data) > 0:
        mms = MinMaxScaler(feature_range=(0,1))
        nulls_data[dense] = mms.fit_transform(nulls_data[dense])
        
    nulls_res = []
    if len(nulls_data) > 0:
        for i in range(10):
            model = pickle.load(open("./comb_ckpt/model_" + str(i) + "_comb_null.pkl", 'rb'))
            nulls_res.append(list(model.predict(nulls_data)))
    nulls_res = np.array(nulls_res)
    y_pred = np.mean(nulls_res, axis=0)
    df_nulls.insert(df_nulls.shape[1], 'pred', y_pred)
    
    combination = pd.concat([df_comb, df_businesses, df_users, df_nulls, df_business_null, df_user_null, df_null]).sort_index()

    #df_test = pd.read_csv(folder_path + 'yelp_val.csv')
    #mse = mean_squared_error(combination['pred'], df_test['stars'])
    #print("Val RMSE: %.4f" % (mse ** 0.5))
    
    
    result_str = "user_id, business_id, prediction\n"
    for index, row in combination.iterrows():
        result_str += row['user_id'] + ',' + row['business_id'] + ',' + str(row['pred']) + '\n'
    with open(output_path, "w") as f:
        f.writelines(result_str)
        
    e_t = time.time()
    print("Duration: ", e_t - s_t)
        