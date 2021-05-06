from __future__ import division
import pyspark
import sys
import json
import re
import time
import math as ma
from itertools import combinations as comb
from collections import defaultdict,Counter


sc=pyspark.SparkContext('local[*]')


start=time.time()
train_rdd=sc.textFile(sys.argv[1]).map(lambda x : json.loads(x))

if(sys.argv[3]=="item_based"):
    
        bus_user=train_rdd.filter(lambda x : x['business_id']!=None and x['user_id']!=None).map(lambda x : (x['business_id'],x['user_id'])).groupByKey().map(lambda x : (x[0],list(set(x[1])))).collectAsMap()

        bur_pair=train_rdd.filter(lambda x : x['business_id']!=None and x['user_id']!=None and x['stars']!=None).map(lambda x : ((x['business_id'],x['user_id']),x['stars'])).collectAsMap()

        bus_ids=train_rdd.filter(lambda x : x['business_id']!=None).map(lambda x : x['business_id']).distinct().sortBy(lambda x : x).collect()

        pairs=list(comb(bus_ids,2))
        #business_pairs=[]
        file=open(sys.argv[2],"w") 

        for ids in pairs:
          
          common=list(set(bus_user[ids[0]]).intersection(set(bus_user[ids[1]])))  
          if(len(common)>=3):
              
              a=[]
              b=[]
              for i in common:
                  a.append(bur_pair[(ids[0],i)])
                  b.append(bur_pair[(ids[1],i)])
              a_avg=sum(a)/len(a)
              b_avg=sum(b)/len(b)
              numer=0;
              denom1=0;
              denom2=0;
              sim=0;
              for j in range(len(a)):
                  numer=numer+((a[j]-a_avg)*(b[j]-b_avg))
                  denom1=denom1+(a[j]-a_avg)**2
                  denom2=denom2+(b[j]-b_avg)**2
              if((ma.sqrt(denom1)*ma.sqrt(denom2))!=0):    
                  sim=numer/(ma.sqrt(denom1)*ma.sqrt(denom2))
                    
              dic={}
            
              if(sim>0):
                    
                dic["b1"]=ids[0]
                dic["b2"]=ids[1]
                dic["sim"]=sim
                #business_pairs.append(dic)
                file.write(json.dumps(dic)+'\n');
        
        
        file.close()

# Number of hash function
# Bands depends on number of rows per bands

        
if(sys.argv[3]=="user_based"):        
      #Should write content related to User-Based
        bus_id_rdd=train_rdd.filter(lambda x : x["business_id"]!=None).map(lambda x : x["business_id"]).distinct().sortBy(lambda x : x).zipWithIndex()
        
        m=bus_id_rdd.count()
        
        bus_id_data_dic= bus_id_rdd.collectAsMap()
        
        trdd=train_rdd.filter(lambda x : x["business_id"]!=None and x["user_id"]!=None).map(lambda x : (x["user_id"],bus_id_data_dic[x["business_id"]])).groupByKey().map(lambda x : (x[0],list(set(x[1]))))

        original_data=train_rdd.filter(lambda x : x["business_id"]!=None and x["user_id"]!=None).map(lambda x : (x["user_id"],x["business_id"])).groupByKey().map(lambda x : (x[0],list(x[1]))).collectAsMap()

        usb_pair=train_rdd.filter(lambda x : x['business_id']!=None and x['user_id']!=None and x['stars']!=None).map(lambda x : ((x['user_id'],x['business_id']),x['stars'])).collectAsMap()
        
        a=11
        b=37
        
        def signature_matrix(x):
            data=x[1] #List of businesses data
            hv=[] # Hash Values
            for i in range(1,51):
                temp=[]
                for j in data:
                    temp.append((a*i*j+b*i)%m);
                hv.append(min(temp))  
            return (x[0],hv)
        
        n=50

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
        
        def pairs(data):
          x=sorted(data[1])
          temp=list(comb(x,2));
          return temp;
    
        def jacsim(dat):
              data=sorted(dat) 
              x=data[0]
              y=data[1]

              userid_1=original_data[x] # List of business ids for each user id
              userid_2=original_data[y] # List of business ids for each user id

              intersection_len=len(set(userid_1) & set(userid_2))
              union_len=len(set(userid_1).union(set(userid_2)))

              sim=intersection_len/union_len
              final_dic={}
              final_dic['u1']=x
              final_dic['u2']=y

              if(sim>=0.01 and intersection_len>=3): 
                  final_dic['sim']=sim
              else:
                 final_dic['sim']=-10;

              return final_dic
    

        def pearson(di):
                x=di['u1']
                y=di['u2']

                common=list(set(original_data[x]).intersection(set(original_data[y])))
                a=[]
                b=[]
                for i in common:
                    a.append(usb_pair[(x,i)])
                    b.append(usb_pair[(y,i)])
                a_avg=sum(a)/len(a)
                b_avg=sum(b)/len(b)
                numer=0;
                denom1=0;
                denom2=0;
                sim=-10;
                for j in range(len(a)):
                    numer=numer+((a[j]-a_avg)*(b[j]-b_avg))
                    denom1=denom1+(a[j]-a_avg)**2
                    denom2=denom2+(b[j]-b_avg)**2
                if((ma.sqrt(denom1)*ma.sqrt(denom2))!=0):    
                    sim=numer/(ma.sqrt(denom1)*ma.sqrt(denom2))

                di['sim']=sim;   
                
                return di;
    
        r=1
        
        lshrdd=hash_rdd.map(lambda x : LSH(x,r)).flatMap(lambda x : x).map(lambda x : (x[1],x[0])).groupByKey().map(lambda x : (x[0],list(x[1]))).filter(lambda x : len(x[1])>1)
        
        final_data=lshrdd.flatMap(pairs).distinct().map(jacsim).filter(lambda x : x['sim']>0).map(pearson).filter(lambda x : x['sim']>0).collect()

        
        
        fi=open(sys.argv[2],"w")

        for i in final_data[0:-1]:
            fi.write(json.dumps(i)+'\n')
            
        fi.write(json.dumps(final_data[-1]))
        fi.close()    
        
 
        
end=time.time()

print(end-start)