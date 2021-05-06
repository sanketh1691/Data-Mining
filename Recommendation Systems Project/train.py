import pyspark
import sys
import re
import time
import json
import numpy as np
import math as ma
import os
import pickle
import csv
import binascii as bina
import xgboost as xg
from itertools import combinations as comb
from collections import defaultdict,Counter

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'


start=time.time()

sc=pyspark.SparkContext('local[*]')


train_rdd=sc.textFile("../resource/asnlib/publicdata/train_review.json").map(lambda x : json.loads(x))

user_rdd=sc.textFile("../resource/asnlib/publicdata/user_avg.json").map(lambda x : json.loads(x))
user_rdd2=sc.textFile("../resource/asnlib/publicdata/user.json").map(lambda x : json.loads(x))


business_rdd=sc.textFile("../resource/asnlib/publicdata/business.json").map(lambda x : json.loads(x))
business_rdd2=sc.textFile("../resource/asnlib/publicdata/business_avg.json").map(lambda x : json.loads(x))

#pc=business_rdd.filter(lambda x : x["postal_code"]!=None and x["postal_code"]!="").map(lambda x : x["postal_code"]).distinct().zipWithIndex().collectAsMap()

train_data=train_rdd.map(lambda x : ((x["user_id"],x["business_id"]),x["stars"])).groupByKey().map(lambda x : (x[0],[sum(x[1])/len(x[1])])).collectAsMap()

user_data=user_rdd2.filter(lambda x : x["fans"]!=None and x["useful"]!=None and x["cool"]!=None and x["funny"]!=None).map(lambda x : (x["user_id"],[x["fans"],x["useful"],x["cool"],x["funny"],x["compliment_cool"],x["compliment_more"],x["compliment_cute"],x["compliment_funny"],x["compliment_profile"],x["compliment_hot"],x["compliment_note"],x["compliment_photos"]])).collectAsMap()

user_avg=user_rdd.take(1)[0]

bus_data=business_rdd.filter(lambda x : x["postal_code"]!=None and x["postal_code"]!="").map(lambda x : (x["business_id"],[x["latitude"],x["longitude"]])).collectAsMap()

bus_avg=business_rdd2.take(1)[0]

for i in train_data:
     
     if (i[0] in user_avg):
        train_data[i].append(user_avg[i[0]])
     else:
        train_data[i].append(0)
   
     if (i[0] in user_data):
        train_data[i]=train_data[i]+user_data[i[0]]
     else:
        train_data[i].append(0)
        train_data[i].append(0)
        train_data[i].append(0)
        train_data[i].append(0)
        train_data[i].append(0)
        train_data[i].append(0)
        train_data[i].append(0)        
        train_data[i].append(0)
        train_data[i].append(0)  
        train_data[i].append(0)
        train_data[i].append(0)          
        train_data[i].append(0)  

     if (i[1] in bus_avg):
        train_data[i].append(bus_avg[i[1]])
     else:
        train_data[i].append(0)

        
#c=0 
# Latitude and Longitude Part

for i in train_data:
   
    if (i[1] in bus_data):
       #print(bus_data[i[1]])
       train_data[i]=train_data[i]+bus_data[i[1]]
    else:
       train_data[i].append(0)
       train_data[i].append(0)

        
          
lr=[0.001, 0.005,0.01,0.05]

regression=xg.XGBRegressor(n_estimators=360,reg_lambda=1,gamma=0.05,max_depth=6)  

x_data=list()
y_data=list()

for i in train_data:
  x_data.append(train_data[i][1:])
  y_data.append(train_data[i][0])
    
                                                                          
   
regression.fit(x_data,y_data)

pickle.dump(regression, open("yelp.pickle.dat", "wb"))

end=time.time()
print(end-start)

