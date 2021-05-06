from __future__ import division
import pyspark
import sys
import json
from itertools import combinations as comb
from collections import defaultdict
import time
import random

sc=pyspark.SparkContext('local[*]')

start=time.time()

train_rdd=sc.textFile(sys.argv[1]).map(lambda x : json.loads(x))
user_id_rdd=train_rdd.filter(lambda x : x["user_id"]!=None).map(lambda x : x["user_id"]).distinct().zipWithIndex()
m=user_id_rdd.count()
user_id_data_dic= user_id_rdd.collectAsMap()

trdd=train_rdd.filter(lambda x : x["business_id"]!=None and x["user_id"]!=None).map(lambda x : (x["business_id"],user_id_data_dic[x["user_id"]])).groupByKey().map(lambda x : (x[0],list(set(x[1]))))

original_data=train_rdd.filter(lambda x : x["business_id"]!=None and x["user_id"]!=None).map(lambda x : (x["business_id"],x["user_id"])).groupByKey().map(lambda x : (x[0],list(x[1]))).collectAsMap()


#a=[3, 7,11, 19, 23, 37, 47, 61, 79, 89]
#b=[11,5, 13, 29, 31, 41, 53, 67, 83, 97]

a=11
b=37
#a=[random.randint(1000,100000) for _ in range(10)]
#b=[random.randint(20001,200000) for _ in range(10)]


hash_funcs=[]
"""
for i in a:
  for j in b:
    hash_funcs.append((i,j))
""" 

def signature_matrix(x):
    data=x[1] #List of users data
    hv=[] # Hash Values
    for i in range(1,101):
        temp=[]
        for j in data:
            temp.append((a*i*j+b*i)%m);
        hv.append(min(temp))  
    

    return (x[0],hv)

"""
    for i in hash_funcs:
       temp=[]
       for j in data:
          temp.append((i[0]*j+i[1])%m);
       hv.append(min(temp))  
    

  
"""

    
  


n=100
# Number of hash function
#bands depends on number of rows per band
def LSH(x,r):
    data=x[1];
    bands=n//r
    band_num=1;
    list_bands=[]
    start=0
    end=r
    
    while(band_num<=bands):
      temp=data[start:end]
      start=start+r
      end=end+r; 
      abc=(x[0],(tuple(temp),band_num))
      band_num=band_num+1;
      list_bands.append(abc)
        
    return list_bands


hash_rdd=trdd.map(signature_matrix)
#signature_matrix_dic=hash_rdd.collectAsMap()


def pairs(data):
  x=data[1]
  temp=list(comb(x,2));
  return temp;


def jacsim(dat):
    
  data=sorted(dat)  
  x=data[0]
  y=data[1]
 
  busid_1=original_data[x]
  busid_2=original_data[y]

  intersection_len=len(set(busid_1).intersection(set(busid_2)))
  union_len=len(set(busid_1).union(set(busid_2)))

  sim=intersection_len/union_len

  final_dic={}
  final_dic['b1']=x
  final_dic['b2']=y
  final_dic['sim']=sim
 
  return final_dic


r=1
lshrdd=hash_rdd.map(lambda x : LSH(x,r)).flatMap(lambda x : x).map(lambda x : (x[1],x[0])).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x : len(x[1])>1)
final_data=lshrdd.flatMap(pairs).distinct().map(jacsim).filter(lambda x : x['sim']>=0.055).collect()


f=open(sys.argv[2],"w")

for i in final_data[0:-1]:
  f.write(json.dumps(i)+'\n')

f.write(json.dumps(final_data[-1]))  
f.close()

end=time.time()
print(end-start)


    