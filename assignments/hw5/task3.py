from blackbox import BlackBox
import csv
import random
import sys
import time

if __name__ == '__main__':
    input_path = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_path = sys.argv[4]
    
    s_t = time.time()
    random.seed(553)
    users_list = []
    n = 0
    bx = BlackBox()
    result_str = "seqnum,0_id,20_id,40_id,60_id,80_id\n"
    for i in range(num_of_asks):
        stream_users = bx.ask(input_path, stream_size)
        for user in stream_users:
            n += 1
            if len(users_list) < 100:
                users_list.append(user)

            elif random.random() < 100 / n:
                users_list[random.randint(0, 99)] = user

            if n % 100 == 0:
                result_str = result_str + str(n) + ',' + users_list[0] + ',' + users_list[20] + ',' + users_list[40] + ',' + users_list[60] + ',' + users_list[80] + '\n'
   
    with open(output_path, 'w') as f:
        f.writelines(result_str)

    e_t = time.time()
    print('Duration: ', e_t - s_t)