from __future__ import division
import pyspark
import sys
import json
import re
import time
import pickle
import math as m
from itertools import permutations as perm
from collections import defaultdict,Counter


sc=pyspark.SparkContext('local[*]')


start=time.time()

with open(sys.argv[2],"rb") as file:
    item_user = pickle.load(file)
file.close()
 
item_profiles=item_user[0]
user_profiles_dic=item_user[1]

test_rdd=sc.textFile(sys.argv[1]).map(lambda x : json.loads(x))


def cosine_similarity(x):
  user=user_profiles_dic[x[0]] # User_id
  item=item_profiles[x[1]] # Business_id (Item)

  inter=set(user).intersection(set(item))
  numerator=len(inter)
  denominator=m.sqrt(len(user))*(m.sqrt(len(item)))

  sim=numerator/denominator
  
  dic={}
  dic["user_id"]=x[0]
  dic["business_id"]=x[1]
  dic["sim"]=sim

  return dic

final_result=test_rdd.map(lambda x : (x['user_id'],x['business_id'])).filter(lambda x : (x[0] in user_profiles_dic.keys()) and (x[1] in item_profiles.keys())).map(cosine_similarity).filter(lambda x : x["sim"]>=0.01).collect()


file=open(sys.argv[3],"w")

for i in final_result[0:-1]:
   file.write(json.dumps(i)+"\n")

file.write(json.dumps(final_result[-1]))

end=time.time()
print("-----------",end-start,"-------------")