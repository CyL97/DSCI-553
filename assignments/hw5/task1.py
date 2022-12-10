from blackbox import BlackBox
import random
import binascii
import csv
import sys
import time

def myhashs(user):
    p = 1e9 + 7
    hash_funcs = []
    a = random.sample(range(1, 69997), 50)
    hash_funcs.append(a)
    b = random.sample(range(1, 69997), 50)
    hash_funcs.append(b)
    
    result = []
    x = int(binascii.hexlify(user.encode('utf8')), 16)
    for i in range(50):
        result.append(((hash_funcs[0][i] * x + hash_funcs[1][i]) % p) % 69997)
    return result

if __name__ == '__main__':
    input_path = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_path = sys.argv[4]
    
    s_t = time.time()
    
    result_str = "Time,FPR\n"
    exist_user = set()
    exist_hash = []

    bx = BlackBox()
    for i in range(num_of_asks):
        stream_users = bx.ask(input_path, stream_size)
        #print(stream_users)
        fp = 0
        for user in stream_users:
            result = myhashs(user)
            #print(result)
            if result in exist_hash:
                if user not in exist_user:
                    fp += 1
            exist_hash.append(result)
            exist_user.add(user)
        #print(i, fp)
        result_str = result_str + str(i) + ',' + str(fp / stream_size) + '\n'
    
    with open(output_path, 'w') as f:
        f.writelines(result_str)
    
    e_t = time.time()
    print('Duration: ', e_t - s_t)