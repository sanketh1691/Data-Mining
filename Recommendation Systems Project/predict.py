import pyspark
import sys
import re
import time
import json
import numpy as np
import math as ma
import os
import pickle
import binascii as bina
import csv
import xgboost as xg
from itertools import combinations as comb
from collections import defaultdict,Counter

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'


start=time.time()

sc=pyspark.SparkContext('local[*]')
sc.setLogLevel("WARN")

sc.setLogLevel("OFF")

"""
train_rdd=sc.textFile("../resource/asnlib/publicdata/train_review.json").map(lambda x : json.loads(x))

train_data=train_rdd.map(lambda x : ((x["user_id"],x["business_id"]),x["stars"])).groupByKey().map(lambda x : (x[0],[sum(x[1])/len(x[1])])).collectAsMap()
"""

user_rdd=sc.textFile("../resource/asnlib/publicdata/user_avg.json").map(lambda x : json.loads(x))
user_rdd2=sc.textFile("../resource/asnlib/publicdata/user.json").map(lambda x : json.loads(x))

business_rdd=sc.textFile("../resource/asnlib/publicdata/business.json").map(lambda x : json.loads(x))
business_rdd2=sc.textFile("../resource/asnlib/publicdata/business_avg.json").map(lambda x : json.loads(x))

#pc=business_rdd.filter(lambda x : x["postal_code"]!=None and x["postal_code"]!="").map(lambda x : x["postal_code"]).distinct().zipWithIndex().collectAsMap()

user_avg=user_rdd.take(1)[0]



user_data=user_rdd2.filter(lambda x : x["fans"]!=None and x["useful"]!=None and x["cool"]!=None and x["funny"]!=None).map(lambda x : (x["user_id"],[x["fans"],x["useful"],x["cool"],x["funny"],x["compliment_cool"],x["compliment_more"],x["compliment_cute"],x["compliment_funny"],x["compliment_profile"],x["compliment_hot"],x["compliment_note"],x["compliment_photos"]])).collectAsMap()

bus_data=business_rdd.map(lambda x : (x["business_id"],[x["latitude"],x["longitude"]])).collectAsMap()

bus_avg=business_rdd2.take(1)[0]
"""
for i in train_data:
     
     if (i[0] in user_avg):
        train_data[i].append(user_avg[i[0]])
     else:
        train_data[i].append(0)
   
     if (i[1] in bus_avg):
        train_data[i].append(bus_avg[i[1]])
     else:
        train_data[i].append(0)

        
#c=0
for i in train_data:
   
    if (i[1] in bus_data):
       #print(bus_data[i[1]])
       train_data[i]=train_data[i]+bus_data[i[1]]
    else:
       train_data[i].append(0)
       train_data[i].append(0)
          
            
regression=xg.XGBRegressor(n_estimators=100,reg_lambda=1,gamma=0,max_depth=6)  

x_data=list()
y_data=list()

for i in train_data:
  x_data.append(train_data[i][1:])
  y_data.append(train_data[i][0])
    
    
regression.fit(x_data,y_data)

"""
######################################################################################
# Test File Starts from here
regression=pickle.load(open("yelp.pickle.dat", "rb"))

test_rdd=sc.textFile(sys.argv[1]).map(lambda x : json.loads(x))

test_data=test_rdd.map(lambda x : (x["user_id"],x["business_id"])).distinct().collect()


testing=list()
x_test=list()

for i in test_data:
  temp=dict()
  temp["user_id"]=i[0]
  temp["business_id"]=i[1]

  testing.append(temp)

  temp2=[]
  if (i[0] in user_avg):
      temp2.append(user_avg[i[0]])
  else:
      temp2.append(0)
        
  if (i[0] in user_data):
      temp2=temp2+user_data[i[0]]
  else:
      temp2.append(0)
      temp2.append(0)
      temp2.append(0)
      temp2.append(0)     
      temp2.append(0)
      temp2.append(0) 
      temp2.append(0) 
      temp2.append(0)
      temp2.append(0)     
      temp2.append(0)
      temp2.append(0) 
      temp2.append(0)     

    
  if (i[1] in bus_avg):
      temp2.append(bus_avg[i[1]])
  else:
      temp2.append(0)  

  if (i[1] in bus_data):
      #print(bus_data[i[1]])
      temp2=temp2+bus_data[i[1]]
  else:
      temp2.append(0)
      temp2.append(0)
    
  x_test.append(temp2)   


y_test=regression.predict(x_test)

file=open(sys.argv[2],"w")

for i in range(len(testing)):
   temper=dict()
   temper = testing[i]
   temper["stars"]=np.float64(y_test[i])
   #print(temper)
   file.write(json.dumps(temper)+"\n");
file.close()   


end=time.time()
print(end-start)

