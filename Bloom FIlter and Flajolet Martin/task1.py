import pyspark
import sys
import re
import time
import datetime
import json
import math as ma
import binascii as bina
import csv
from itertools import combinations as comb
from collections import defaultdict,Counter

sc=pyspark.SparkContext('local[*]')

sc.setLogLevel("ERROR")
sc.setLogLevel("OFF")

start=time.time()
bus_frdd=sc.textFile(sys.argv[1]).map(lambda x : json.loads(x))

busf=bus_frdd.filter(lambda x : x['name']!=None).map(lambda x : x['name']).distinct().map(lambda x : int(bina.hexlify(x.encode('utf8')),16)).collect()


num_hash=7;
a=17      
b=53
m=int((len(busf)* num_hash)/ma.log10(2))


bit_array=[0 for i in range(m)]

for x in busf:

  for i in range(1,num_hash+1):
    temp=((a*i*x)+(b*i))%m
    #print(temp)
    bit_array[temp]=1
    #print( bit_array);

fil=open(sys.argv[2],"r")
buss=list()

for i in fil:
  temp=json.loads(i);
  if (temp["name"]!="" or temp["name"]!=None):
      buss.append(temp["name"])  
    
buss=list(buss)
for j in range(len(buss)):
  buss[j]=int(bina.hexlify(buss[j].encode('utf8')),16)  


predictions=[];
for x in buss:
  val="T"
  for i in range(1,num_hash+1):
    temp=((a*i*x)+(b*i))%m
    if bit_array[temp]!=1:
       val="F"
       #print("F")
       break;
  predictions.append(val);  


file=open(sys.argv[3],"w")
file.write(" ".join(predictions))
file.close()

end=time.time()
print(end-start)

