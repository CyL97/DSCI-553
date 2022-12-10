from blackbox import BlackBox
import random
import binascii
import csv
import sys
import time

def myhashs(user):
    p = 1e9 + 7
    hash_funcs = []
    a = random.sample(range(1, 997), 50)
    hash_funcs.append(a)
    b = random.sample(range(1, 997), 50)
    hash_funcs.append(b)
    
    result = []
    x = int(binascii.hexlify(user.encode('utf8')), 16)
    for i in range(50):
        result.append(((hash_funcs[0][i] * x + hash_funcs[1][i]) % p) % 997)
    return result

if __name__ == '__main__':
    input_path = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_path = sys.argv[4]
    
    s_t = time.time()
    
    result_str = "Time,Ground Truth,Estimation\n"
    gt_t = 0
    est_t = 0
    bx = BlackBox()
    for i in range(num_of_asks):
        stream_users = bx.ask(input_path, stream_size)
        
        gt = set()
        exist_hash = []
        for user in stream_users:
            result = myhashs(user)
            if user not in gt:
                gt.add(user)
            exist_hash.append(result)
        
        est_sum = 0
        for j in range(50):
            temp = []
            for value in exist_hash:
                temp.append(int(value[j]))
            #print(temp)
            max_t_zero = 0
            for value in temp:
                temp_str = bin(value)[2:]
                wo_zero = temp_str.rstrip('0')
                if max_t_zero < len(temp_str) - len(wo_zero):
                    max_t_zero = len(temp_str) - len(wo_zero)
            est_sum += 2 ** max_t_zero
        est = est_sum // 50
        gt_t += len(gt)
        est_t += est
        result_str = result_str + str(i) + ',' + str(len(gt)) + ',' + str(est) + '\n'
    
    print(est_t/gt_t)
    with open(output_path, 'w') as f:
        f.writelines(result_str)
    
    e_t = time.time()
    print('Duration: ', e_t - s_t)