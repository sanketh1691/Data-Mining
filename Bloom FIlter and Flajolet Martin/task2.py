import pyspark
import sys
import re
import time
import json
import math as ma
import binascii as bina
import csv 
import datetime as dt
import random as rn
from pyspark.streaming import StreamingContext
from itertools import combinations as comb
from collections import defaultdict,Counter

start=time.time()

sc=pyspark.SparkContext('local[*]')

sc.setLogLevel("ERROR")
sc.setLogLevel("OFF")

ssc=StreamingContext(sc,5)

port_num=int(sys.argv[1])

#java -cp <generate_stream.jar file path> StreamSimulation <business.json file path> 9999 100



hash_funcs=150
total_groups=30

jump=hash_funcs//total_groups

#abc=[rn.randint(1,250) for i in range(20)]
#xyz=[rn.randint(1,250) for i in range(20)]
#abc=[203, 7, 180, 8, 137, 5, 38, 132, 192, 44, 85, 98, 1, 45, 70, 176, 123, 77, 159, 195] 
#xyz=[226, 52, 67, 208, 109, 182, 59, 228, 201, 31, 219, 131, 31, 93, 233, 136, 25, 36, 61, 174]
abc=[210, 124, 58, 25, 53, 2, 225, 138, 235, 48, 201, 13, 213, 186, 20, 195, 97, 29, 80, 198]        
xyz=[144, 204, 40, 53, 110, 247, 18, 71, 61, 152, 109, 119, 1, 145, 140, 144, 52, 209, 122, 191]  

print (abc)
print (xyz)

hash_funcs_list=[]

for i in abc:
    for j in xyz:
        hash_funcs_list.append((i,j))
        
        
#a=17
#b=29
a=1
b=1
m_val=150


def fm(data):
    
    raw_data=data.collect()
    
    states=list()
    for rd in raw_data:
        temp=json.loads(rd)
        st=temp["state"]
        
        if st!=None and st!="":
            states.append(st)
            
    
    ct=dt.datetime.now()
    timestamp=ct.strftime("%Y-%m-%d %H:%M:%S")
    ground_truth=len(set(states))
    
    for i in range(len(states)):
        states[i]=int(bina.hexlify(states[i].encode('utf8')),16)
   
    values=[];
    
    #for i in range(1,hash_funcs+1):
    for i in hash_funcs_list[0:150]:
        max_zeros=0;
        for j in states:
            temp=((a*i[0]*j)+(b*i[1]))%m_val
            tstr='{0:b}'.format(temp)
            trail_zeros=len(tstr)-len(tstr.rstrip("0"))
            
            max_zeros=max(max_zeros,trail_zeros)
            
        values.append(2**max_zeros)    
    
    average_values=[]
    strt=0
    #total_groups=sorted(total_groups)

    for i in range(total_groups):
        
            temp=values[strt:strt+jump]
            
            temp2=sum(temp)/len(temp)
            
            average_values.append(temp2)
            
            strt=strt+jump
            
    average_values=sorted(average_values)
    indexs=total_groups//2
    
    if total_groups%2==0:
        
        final_val=(average_values[indexs]+average_values[indexs-1])//2
    else:
        final_val=average_values[indexs]
    
    #print([timestamp,ground_truth,final_val])
    
    temp3=float(ground_truth-final_val)/float(ground_truth)
    #print(temp3)
    
    file=open(sys.argv[2],"a")
    
    f_w=csv.writer(file)
    f_w.writerow([timestamp,ground_truth,int(final_val)])
   
    
    return ;
    #return final_val;
    #Have to write it in to a file
        
        
file=open(sys.argv[2],"w")
f_w=csv.writer(file)
f_w.writerow(["Time","Ground Truth","Estimation"]);


#final_data=[]
data=ssc.socketTextStream("localhost", port_num)

stream_rdd=data.window(30,10)

stream_rdd.foreachRDD(fm)



end=time.time()

print(end-start)

#print(stream_rdd.take(4))
ssc.start()
ssc.awaitTermination()
