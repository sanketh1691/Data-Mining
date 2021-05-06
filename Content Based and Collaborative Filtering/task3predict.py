import pyspark
import sys
import json
import re
import time
import math as m
from itertools import combinations as comb
from collections import defaultdict,Counter


sc=pyspark.SparkContext('local[*]')


start=time.time()
train_rdd=sc.textFile(sys.argv[1]).map(lambda x : json.loads(x))

test_rdd=sc.textFile(sys.argv[2]).map(lambda x : json.loads(x)).map(lambda x : (x['user_id'],x['business_id']))
test_data=test_rdd.collect()



def bus_rating_calulation(lis):
    res=defaultdict(lambda : [])

    for i in lis:
       res[i[0]].append(i[1]) 
    for i in res:
       res[i]=sum(res[i])/len(res[i])
    
    return dict(res); 

if (sys.argv[5]=="item_based"):
    
    file=open(sys.argv[3],"r")

    train_model=defaultdict(lambda : 0.0)
    for di in file:
      temp=json.loads(di)
      train_model[(temp['b1'],temp['b2'])]= train_model[(temp['b1'],temp['b2'])] + temp['sim']

    train_model=dict(train_model)  


    user_bus=train_rdd.filter(lambda x : x['business_id']!=None and x['user_id']!=None and x['stars']!=None).map(lambda x : (x['user_id'],(x['business_id'],x['stars']))).groupByKey().map(lambda x : (x[0],list(x[1]))).mapValues(bus_rating_calulation).collectAsMap()
    
    bus_avg=train_rdd.filter(lambda x: x['business_id']!=None and x['stars']!=None).map(lambda x : (x['business_id'],x['stars'])).groupByKey().map(lambda x : (x[0],list(x[1]))).map(lambda x : (x[0],sum(x[1])/len(x[1]))).collectAsMap()
    

    fil=open(sys.argv[4],"w")
    
    n=7
    for data in test_data:
       user=data[0]
       item=data[1]
       result={}
       pre=0;
    
       if user in user_bus:
           calculate_dic={}
           ratings={}
           dic=user_bus[user];
    
           for it in dic:
              temp=tuple(sorted((it,item)))
    
              if(temp in train_model):
                  calculate_dic[temp]=train_model[temp];
                  ratings[temp]=dic[it];
    
           values=sorted(calculate_dic.items(),key=lambda x : x[1],reverse=True)[0:n];  
    
           num=0;
           den=0;
           for v in values:
    
              num=num+(v[1]*ratings[v[0]])
              den=den+v[1]
           if(den<=0 or num<=0):
              pre=bus_avg[item] if (item in bus_avg) else 0;
              #pre=0
           else:   
              pre=num/den;
    
       else:
          pre=bus_avg[item] if (item in bus_avg) else 0;     
          #pre=0;
    
       result['user_id']=user
       result['business_id']=item
       result['stars']=pre
    
       fil.write(json.dumps(result)+"\n");
    
    #final_result=test_rdd.map(item_prediction).collect()
    
    fil.close()    
    
   
if (sys.argv[5]=="user_based"):
    
    file=open(sys.argv[3],"r")

    train_model=defaultdict(lambda : 0.0)
    for di in file:
      temp=json.loads(di)
      train_model[(temp['u1'],temp['u2'])]= train_model[(temp['u1'],temp['u2'])] + temp['sim']

    train_model=dict(train_model)       
    
    bus_user=train_rdd.filter(lambda x : x['business_id']!=None and x['user_id']!=None and x['stars']!=None).map(lambda x : (x['business_id'],(x['user_id'],x['stars']))).groupByKey().map(lambda x : (x[0],list(x[1]))).mapValues(bus_rating_calulation).collectAsMap()
    
    #files=open("/resource/asnlib/publicdata/user_avg.json","r")
    
    #user_avg=eval(files.read())
    user_avg=train_rdd.filter(lambda x: x['user_id']!=None and x['stars']!=None).map(lambda x : (x['user_id'],x['stars'])).groupByKey().map(lambda x : (x[0],list(x[1]))).map(lambda x : (x[0],sum(x[1])/len(x[1]))).collectAsMap()

    
    fil=open(sys.argv[4],"w")
    
    n=9
    for data in test_data:
        user=data[0]
        item=data[1]
        result={}
        pre=0;

        if item in bus_user:
            calculate_dic={}
            ratings={}
            dic=bus_user[item];

            for it in dic:
              temp=tuple(sorted((it,user)))

              if(temp in train_model):
                  calculate_dic[temp]=train_model[temp];
                  ratings[temp]=dic[it]-user_avg[it];

            values=sorted(calculate_dic.items(),key=lambda x : x[1],reverse=True)[0:n];  

            num=0;
            den=0;
            for v in values:

              num=num+(v[1]*ratings[v[0]])
              den=den+v[1]

            user_average= user_avg[user] if (user in user_avg) else 0;

            if(den<=0 or num<=0):
              pre=user_avg[user] if (user in user_avg) else 0;
              #pre=0
            else:   
              pre=user_average+(num/den);

        else:
          pre=user_avg[user] if (user in user_avg) else 0;
          #pre=0;

        result['user_id']=user
        result['business_id']=item
        result['stars']=pre

        fil.write(json.dumps(result)+"\n");
    
    fil.close()    

      
        
end=time.time()

print(end-start)