import sys
import time
from sklearn.cluster import KMeans
import numpy as np

if __name__ == '__main__':
    input_path = sys.argv[1]
    n_cluster = int(sys.argv[2])
    output_path = sys.argv[3]
    s_t = time.time()
    
    rs = set()
    ds_dict = {}
    ds_c_dict = {}
    ds_d_dict = {}
    ds_point_dict = {}
    cs_dict = {}
    cs_c_dict = {}
    cs_d_dict = {}
    cs_point_dict = {}
    D = 0
    
    with open(input_path, "r") as f:
        datas = np.array(f.readlines())
    
    npdata = []
    for data in datas:
        temp = np.array(data.strip('\n').split(','))
        npdata.append(temp)
    npdata = np.array(npdata).astype(np.float64)
    
    #step1
    np.random.shuffle(npdata)
    npdata = np.array_split(npdata, 5)
    data1 = npdata[0]
    
    #step2
    kmeans1 = KMeans(n_clusters=5 * n_cluster).fit(npdata[0][:, 2:])
    
    #step3
    clusters = {}
    for idx, cid in enumerate(kmeans1.labels_):
        if cid not in clusters.keys():
            clusters[cid] = []
        clusters[cid].append(idx)
    rs = set()
    #print(len(data1))
    for idx in clusters.values():
        if len(idx) == 1:
            #print(idx)
            rs.add(idx[0])
    ds = np.delete(data1, list(rs), axis=0)
    #print(len(data1))
    #print(len(ds))
    
    #step4
    kmeans2 = KMeans(n_clusters=n_cluster).fit(ds[:, 2:])
    clusters = {}
    for idx, cid in enumerate(kmeans2.labels_):
        if cid not in clusters.keys():
            clusters[cid] = []
        clusters[cid].append(idx)
    
    #step5
    for cid, idx in clusters.items():
        n = len(idx)
        ds_feature = ds[idx, 2:]
        SUM = np.sum(ds_feature, axis=0)
        SUMSQ = np.sum(np.square(ds_feature), axis=0)
        ds_dict[cid] = []
        ds_dict[cid].append(n)
        ds_dict[cid].append(SUM)
        ds_dict[cid].append(SUMSQ)
        
        points = np.array(ds[idx, 0]).astype(int).tolist()
        ds_point_dict[cid] = points
        
        centroid = SUM / n
        ds_c_dict[cid] = centroid
        
        d = np.sqrt(np.subtract(SUMSQ / n, np.square(centroid)))
        ds_d_dict[cid] = d
    
    #step6
    data_rs = data1[list(rs), :]
    if len(rs) >= 5 * n_cluster:
        kmeans_temp = KMeans(n_clusters=5 * n_cluster).fit(data_rs[:, 2:])
        clusters = {}
        for idx, cid in enumerate(kmeans_temp.labels_):
            if cid not in clusters.keys():
                clusters[cid] = []
            clusters[cid].append(idx)
        rs = set()
        #print(len(data1))
        for idx in clusters.values():
            if len(idx) == 1:
                #print(idx)
                rs.add(idx[0])
                
        for cid, idx in clusters.items():
            if len(idx) > 1:
                n = len(idx)
                cs_feature = data_rs[idx, 2:]
                SUM = np.sum(cs_feature, axis=0)
                SUMSQ = np.sum(np.square(cs_feature), axis=0)
                cs_dict[cid] = []
                cs_dict[cid].append(n)
                cs_dict[cid].append(SUM)
                cs_dict[cid].append(SUMSQ)
                
                points = np.array(data_rs[idx, 0]).astype(int).tolist()
                cs_point_dict[cid] = points
                
                centroid = SUM / n
                cs_c_dict[cid] = centroid
                
                d = np.sqrt(np.subtract(SUMSQ / n, np.square(centroid)))
                cs_d_dict[cid] = d
    #print(cs_dict)
    with open(output_path, "w") as f:
        f.write('The intermediate results:\n')
        num_ds = 0
        num_cs = 0
        for value in ds_dict.values():
            num_ds += value[0]
        for value in cs_dict.values():
            num_cs += value[0]
        result_str = 'Round 1: ' + str(num_ds) + ',' + str(len(cs_dict)) + ',' + str(num_cs) + ',' + str(len(rs)) + '\n'
        f.write(result_str)
    
    #step7-12
    D = 2 * np.sqrt(data1.shape[1] - 2)
    for i in range(2, 6):
        #step7
        for idx, value in enumerate(npdata[i - 1]):
            data = value[2:]
            #step8
            maxx = float('inf')
            cluster = -1
            for cid, values in ds_dict.items():
                mahalanobis_dis = np.sqrt(np.sum(np.square(np.divide(np.subtract(data, ds_c_dict[cid]), ds_d_dict[cid])), axis=0))
                if mahalanobis_dis < maxx:
                    maxx = mahalanobis_dis
                    cluster = cid
                    
            if maxx < D and cluster != -1:
                n = ds_dict[cluster][0] + 1
                SUM = np.add(ds_dict[cluster][1], data)
                SUMSQ = np.add(ds_dict[cluster][2], np.square(data))
                ds_dict[cluster] = []
                ds_dict[cluster].append(n)
                ds_dict[cluster].append(SUM)
                ds_dict[cluster].append(SUMSQ)

                centroid = SUM / n
                ds_c_dict[cluster] = centroid

                d = np.sqrt(np.subtract(SUMSQ / n, np.square(centroid)))
                ds_d_dict[cluster] = d
                
                ds_point_dict[cluster].append(int(value[0]))
            else:
                #step9
                maxx = float('inf')
                cluster = -1
                for cid, values in cs_dict.items():
                    mahalanobis_dis = np.sqrt(np.sum(np.square(np.divide(np.subtract(data, cs_c_dict[cid]), cs_d_dict[cid])), axis=0))
                    if mahalanobis_dis < maxx:
                        maxx = mahalanobis_dis
                        cluster = cid
                if maxx < D and cluster != -1:
                    n = cs_dict[cluster][0] + 1
                    SUM = np.add(cs_dict[cluster][1], data)
                    SUMSQ = np.add(cs_dict[cluster][2], np.square(data))
                    cs_dict[cluster] = []
                    cs_dict[cluster].append(n)
                    cs_dict[cluster].append(SUM)
                    cs_dict[cluster].append(SUMSQ)

                    centroid = SUM / n
                    cs_c_dict[cluster] = centroid

                    d = np.sqrt(np.subtract(SUMSQ / n, np.square(centroid)))
                    cs_d_dict[cluster] = d

                    cs_point_dict[cluster].append(int(value[0]))
                else:
                    #step10
                    rs.add(idx)
        #step11
        data_rs = npdata[i - 1][list(rs), :]
        if len(rs) >= 5 * n_cluster:
            kmeans_temp = KMeans(n_clusters=5 * n_cluster).fit(data_rs[:, 2:])
            clusters = {}
            for idx, cid in enumerate(kmeans_temp.labels_):
                if cid not in clusters.keys():
                    clusters[cid] = []
                    clusters[cid].append(idx)
            rs = set()
            #print(len(data1))
            for idx in clusters.values():
                if len(idx) == 1:
                    #print(idx)
                    rs.add(idx[0])
                    
            for cid, idx in clusters.items():
                if len(idx) > 1:
                    n = len(idx)
                    cs_feature = data_rs[idx, 2:]
                    SUM = np.sum(cs_feature, axis=0)
                    SUMSQ = np.sum(np.square(cs_feature), axis=0)
                    cs_dict[cid] = []
                    cs_dict[cid].append(n)
                    cs_dict[cid].append(SUM)
                    cs_dict[cid].append(SUMSQ)
                    
                    points = np.array(data_rs[idx, 0]).astype(int).tolist()
                    cs_point_dict[cid] = points
                    
                    centroid = SUM / n
                    cs_c_dict[cid] = centroid

                    d = np.sqrt(np.subtract(SUMSQ / n, np.square(centroid)))
                    cs_d_dict[cid] = d

        #step12
        new_dict = {}
        for cid1 in cs_dict.keys():
            cluster = -1
            for cid2 in cs_dict.keys():
                if cid1 != cid2:
                    mahalanobis_dis1 = np.sqrt(np.sum(np.square(np.divide(np.subtract(cs_c_dict[cid1], cs_c_dict[cid2]), cs_d_dict[cid2], out=np.zeros_like(np.subtract(cs_c_dict[cid1], cs_c_dict[cid2])), where=cs_d_dict[cid2] != 0)), axis=0))
                    mahalanobis_dis2 = np.sqrt(np.sum(np.square(np.divide(np.subtract(cs_c_dict[cid2], cs_c_dict[cid1]), cs_d_dict[cid1], out=np.zeros_like(np.subtract(cs_c_dict[cid2], cs_c_dict[cid1])), where=cs_d_dict[cid1] != 0)), axis=0))
                    mahalanobis_dis = min(mahalanobis_dis1, mahalanobis_dis2)
                    if mahalanobis_dis < D:
                        D = mahalanobis_dis
                        cluster = cid2
            new_dict[cid1] = cluster
        for cid1, cid2 in new_dict.items():
            if cid1 in cs_dict and cid2 in cs_dict:
                if cid1 != cid2:
                    n = cs_dict[cid1][0] + cs_dict[cid2][0]
                    SUM = np.add(cs_dict[cid1][1], cs_dict[cid2][1])
                    SUMSQ = np.add(cs_dict[cid1][2], cs_dict[cid2][2])
                    cs_dict[cid2][0] = n
                    cs_dict[cid2][1] = SUM
                    cs_dict[cid2][2] = SUMSQ
                    
                    centroid = SUM / n
                    cs_c_dict[cid2] = centroid
                    
                    d = np.sqrt(np.subtract(SUMSQ / n, np.square(centroid)))
                    cs_d_dict[cid2] = d

                    cs_point_dict[cid2].extend(cs_point_dict[cid1])
           
                    cs_dict.pop(cid2)
                    cs_c_dict.pop(cid2)
                    cs_d_dict.pop(cid2)
                    cs_point_dict.pop(cid2)
        #step13 last iteration
        if i == 5:
            new_dict = {}
            for cid1 in cs_dict.keys():
                cluster = -1
                for cid2 in ds_dict.keys():
                    if cid1 != cid2:
                        mahalanobis_dis1 = np.sqrt(np.sum(np.square(np.divide(np.subtract(cs_c_dict[cid1], ds_c_dict[cid2]), ds_d_dict[cid2], out=np.zeros_like(np.subtract(cs_c_dict[cid1], ds_c_dict[cid2])), where=ds_d_dict[cid2] != 0)), axis=0))
                        mahalanobis_dis2 = np.sqrt(np.sum(np.square(np.divide(np.subtract(ds_c_dict[cid2], cs_c_dict[cid1]), cs_d_dict[cid1], out=np.zeros_like(np.subtract(ds_c_dict[cid2], cs_c_dict[cid1])), where=cs_d_dict[cid1] != 0)), axis=0))
                        mahalanobis_dis = min(mahalanobis_dis1, mahalanobis_dis2)
                        if mahalanobis_dis < D:
                            D = mahalanobis_dis
                            cluster = cid2
                new_dict[cid1] = cluster
            for cid1, cid2 in new_dict.items():
                if cid1 in cs_dict and cid2 in ds_dict:
                    if cid1 != cid2:
                        n = cs_dict[cid1][0] + ds_dict[cid2][0]
                        SUM = np.add(cs_dict[cid1][1], ds_dict[cid2][1])
                        SUMSQ = np.add(cs_dict[cid1][2], ds_dict[cid2][2])
                        ds_dict[cid2][0] = n
                        ds_dict[cid2][1] = SUM
                        ds_dict[cid2][2] = SUMSQ

                        centroid = SUM / n
                        ds_c_dict[cid2] = centroid

                        d = np.sqrt(np.subtract(SUMSQ / n, np.square(centroid)))
                        ds_d_dict[cid2] = d

                        ds_point_dict[cid2].extend(cs_point_dict[cid1])

                        cs_dict.pop(cid1)
                        cs_c_dict.pop(cid1)
                        cs_d_dict.pop(cid1)
                        cs_point_dict.pop(cid1)

        with open(output_path, "a") as f:
            num_ds = 0
            num_cs = 0
            for value in ds_dict.values():
                num_ds += value[0]
            for value in cs_dict.values():
                num_cs += value[0]
            result_str = 'Round ' + str(i) + ': ' + str(num_ds) + ',' + str(len(cs_dict)) + ',' + str(num_cs) + ',' + str(len(rs)) + '\n'
            f.write(result_str)
    if len(rs) > 0:
        data_rs = npdata[4][list(rs), 0]
        rs = set([int(n) for n in data_rs])
    result = {}
    for cid in ds_dict.keys():
        for point in ds_point_dict[cid]:
            result[point] = cid
    for cid in cs_dict.keys():
        for point in cs_point_dict[cid]:
            result[point] = -1
    for point in rs:
        result[point] = -1

    with open(output_path, "a") as f:
        f.write('\n')
        f.write('The clustering results:\n')
        order_dict = sorted(result.keys(), key=int)
        for point in order_dict:
            f.write(str(point) + ',' + str(result[point]) + '\n')           
                 
    e_t = time.time()
    print('Duration: ', e_t - s_t)