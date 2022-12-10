from pyspark import SparkContext
from itertools import permutations
from collections import defaultdict
from copy import deepcopy
import sys
import time

def GN(g, v_list):
    bet = defaultdict(float)
    for root in v_list:
        parent = defaultdict(set)
        lvl = {}
        n_sp = defaultdict(float)
        path = []
        queue = []
        queue.append(root)
        vis = set()
        vis.add(root)
        lvl[root] = 0
        n_sp[root] = 1
        
        while len(queue) > 0:
            root = queue.pop(0)
            path.append(root)
            for p in g[root]:
                if p not in vis:
                    queue.append(p)
                    vis.add(p)
                    if p not in parent.keys():
                        parent[p] = set()
                        parent[p].add(root)
                    else:
                        parent[p].add(root)
                    n_sp[p] += n_sp[root]
                    lvl[p] = lvl[root] + 1
                elif lvl[p] == lvl[root] + 1:
                    if p not in parent.keys():
                        parent[p] = set()
                        parent[p].add(root)
                    else:
                        parent[p].add(root)
                    n_sp[p] += n_sp[root]
        v_w = {}
        for p in path:
            v_w[p] = 1
        edge_w = defaultdict(float)
        for p in reversed(path):
            for q in parent[p]:
                temp_w = v_w[p] * (n_sp[q] / n_sp[p])
                v_w[q] += temp_w
                if tuple(sorted([p, q])) not in edge_w.keys():
                    edge_w[tuple(sorted([p, q]))] = temp_w
                else:
                    edge_w[tuple(sorted([p, q]))] += temp_w
        for key, value in edge_w.items():
            if key not in bet.keys():
                bet[key] = value / 2
            else:
                bet[key] += value / 2
    bet = sorted(bet.items(), key=lambda x: (-x[1], x[0]))
    #list
    return bet
    
if __name__ == '__main__':
    t = sys.argv[1]
    input_path = sys.argv[2]
    between_path = sys.argv[3]
    output_path = sys.argv[4]
    
    s_t = time.time()
    
    spark = SparkContext('local[*]', 'task2')
    spark.setLogLevel('ERROR')
    lines = spark.textFile(input_path)
    first = lines.first() 
    lines = lines.filter(lambda row: row != first).map(lambda row: row.split(","))
    user_bus = lines.groupByKey().mapValues(set)
    user_bus_dict = {}
    for user, bus in user_bus.collect():
        user_bus_dict[user] = bus
    
    users = lines.map(lambda row: row[0]).distinct()
    candidates = list(permutations(users.collect(),2))
    edges = []
    v_set = set()
    for candidate in candidates:
        temp = user_bus_dict[candidate[0]] & user_bus_dict[candidate[1]]
        if len(temp) >= int(t):
            edges.append(candidate)
            v_set.add(candidate[0])
    v_list = list(v_set)
    
    g = {}
    for user1, user2 in edges:
        if user1 not in g.keys():
            g[user1] = set()
            g[user1].add(user2)
        else:
            g[user1].add(user2)
        if user2 not  in g.keys():
            g[user2] = set()
            g[user2].add(user1)
        else:
            g[user2].add(user1)
    #[(tuple,float)]
    bet = GN(g, v_list)
    
    with open(between_path, "w") as f:
        for user, value in bet:
            user = str(user)
            temp_str = user + "," + str(round(value, 5)) + "\n" 
            f.write(temp_str)    
 
    sub_g = deepcopy(g)
    n_edge = len(bet)
    k = {n: len(g[n]) for n in g}
    maxx = -float('inf')

    while len(bet) > 0:
        candidates = []
        v_s = v_list.copy()
        while len(v_s) > 0:
            root = v_s.pop()
            queue = []
            queue.append(root)
            vis = set()
            vis.add(root)
            while len(queue) > 0:
                root = queue.pop(0)
                for p in sub_g[root]:
                    if p not in vis:
                        v_s.remove(p)
                        queue.append(p)
                        vis.add(p)
            vis_list = sorted(list(vis))
            candidates.append(vis_list)
        #print(len(candidates))
        modularity = 0.0
        for candidate in candidates:
            for p in candidate:
                for q in candidate:
                    a_p_q = 1.0 if q in g[p] else 0.0
                    modularity += a_p_q - (k[p] * k[q]) / (2.0 * n_edge)
        modularity /= (2 * n_edge)

        if modularity > maxx:
            #print(len(candidates))
            maxx = modularity
            result = deepcopy(candidates)

        highest_betweenness = bet[0][1]
        for v, value in bet:
            if value >= highest_betweenness:
                sub_g[v[0]].remove(v[1])
                sub_g[v[1]].remove(v[0])
                
        bet = GN(sub_g, v_list)
        #print(len(bet))
    result = sorted(result, key=lambda x: (len(x), x[0]))
    
    with open(output_path, "w") as f:
        for user in result:
            user = str(user)
            temp_str = user[1:-1] + "\n" 
            f.write(temp_str)
    
    e_t = time.time()
    print('Duration: ', e_t - s_t)