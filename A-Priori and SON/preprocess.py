import pyspark
import sys
import csv
import json
from itertools import combinations as comb
from collections import defaultdict




sc=pyspark.SparkContext('local[*]')

busrdd = sc.textFile("yelp_academic_dataset_business.json")
bus_rdd=busrdd.map(lambda x : json.loads(x)).filter(lambda x : x['stars']>=4.0 and x['business_id']!=None and x['state']!=None)


revrdd = sc.textFile("yelp_academic_dataset_review.json")
rev_rdd=revrdd.map(lambda x : json.loads(x)).filter(lambda x : x['business_id']!=None and x['user_id']!=None)


cus_rdd=rev_rdd.map(lambda x : (x['business_id'],x['user_id'])).join(bus_rdd.map(lambda x : (x['business_id'],x['state']))).map(lambda x : x[1])


cus_data=cus_rdd.collect()

file=open("user_state.csv","w");
f_w=csv.writer(file)
f_w.writerow(["user_id","state"]);

with file:
  f_w.writerows(cus_data)




