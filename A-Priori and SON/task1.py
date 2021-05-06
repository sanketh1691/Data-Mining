import pyspark
import sys
import csv
import json
from itertools import combinations as comb
from collections import defaultdict
import time

start=time.time()


sc=pyspark.SparkContext('local[*]')


smallrdd = sc.textFile(sys.argv[3])
s_rdd = smallrdd.map(lambda x : x.split(','))


def get_frequent_sets(x,size):
  
  comb_sets=list(comb(x,2)) # Generate itemsets of pairs of (size-1)
  
  itemsets_size_cand=[]
  for i in comb_sets:
    temp=tuple(set(i[0]).union(set(i[1])))
    if(len(temp)==size):
      temp=tuple(sorted(temp))
      if(temp not in itemsets_size_cand):
         itemsets_size_cand.append(temp)
  #print(itemsets_size_cand)    
  return itemsets_size_cand;



partitions=s_rdd.getNumPartitions()
threshold=int(sys.argv[2])
#print(partitions)
def A_Priori(x,total_len):
  #print(len(x));
    x=list(x)
    item_1 = list(set(i for sub in x for i in sub))
    itemset_1=list(comb(item_1,1))
    frac_threshold = int(threshold/partitions)
    #print(frac_threshold)
    temp=defaultdict(lambda : 0);

    for i in itemset_1:
      for j in x:
        if set(i).issubset(set(j)):
          temp[i]=temp[i]+1

    length=float(len(x));
    itemsets=[] # Final List of itemsets of each size
    itemset_1=[] # List of frequent itemsets of size 1
    for i in temp:
      if(temp[i]>=(frac_threshold)):
        itemset_1.append(i);
    itemset_1=sorted(itemset_1);
    itemsets.append(itemset_1)
    #print(itemset_1)        
    
    if(len(itemset_1)>0):
        item_2=comb(itemset_1,2); # List of candidate itemdsets of size 2
        itemset_2=[]
        for k in item_2:
          xyz=tuple(set(k[0]).union(set(k[1])))
          xyz=tuple(sorted(xyz))
          itemset_2.append(xyz); # List of candidate itemdsets of size 2
        
        temp=defaultdict(lambda : 0);

        
        for i in itemset_2:
          for j in x:
            if set(i).issubset(set(j)):
              temp[i]=temp[i]+1

        itemset_2_freq=[] # Frequent itemsets of size 2 
        for i in temp:
          if(temp[i]>=(frac_threshold)):
            itemset_2_freq.append(i); 
        itemsets.append(itemset_2_freq) 
      #print(itemsets[0])
      #print(itemsets[1])   

    else:
      return itemsets;



    size=3;
    while(len(itemsets[size-2])>0):

        lis_k=get_frequent_sets(itemsets[size-2],size);
        count_dic=defaultdict(lambda : 0);
        freq_sets=[]
   
        for i in lis_k:
          for j in x:
            if set(i).issubset(set(j)):
               count_dic[i]=count_dic[i]+1;

        for i in count_dic:
          if(count_dic[i]>=frac_threshold):
            freq_sets.append(i); 
        
        #cand_sets.sort()
        itemsets.append(freq_sets)
        size=size+1;
    
    return itemsets[0:-1];



header=s_rdd.first()

if(int(sys.argv[1])==1):
    srdd = s_rdd.filter(lambda x: x!= header).groupByKey().map(lambda x : list(set(x[1])))
elif(int(sys.argv[1])==2):
    srdd = s_rdd.filter(lambda x: x!= header).map(lambda x : (x[1],x[0])).groupByKey().map(lambda x : list(set(x[1])))

total_len=srdd.count()

candidate_itemsets=srdd.mapPartitions(lambda x : A_Priori(x,total_len)).flatMap(lambda x : x).distinct().collect()
# Writing in to file

oneset=[]
result=defaultdict(lambda : [])
for i in candidate_itemsets:
  if(len(i)==1):
    i=str(i)
    oneset.append(i[0:-2]+i[-1]);
    #oneset.append("("+i+")")
  else:
    result[len(i)].append(str(i))

f=open(sys.argv[4],"w");
f.write("Candidates:");
f.write("\n")

if(oneset):
    oneset=sorted(oneset)
    temp=",".join(oneset)
    f.write(str(temp))
    f.write("\n\n")
temp=list(result.keys());
temp.sort()

for i in temp:
  temp_str=",".join(sorted(result[i]));
  f.write(str(temp_str))
  f.write("\n\n")

"""
def SON_phase2(x,candidate_itemsets):
    x=list(x)
    count_dic=defaultdict(lambda : 0);
    candidate_itemsets=list(set(candidate_itemsets))

     
    for i in candidate_itemsets:
      for j in x:
        if(isinstance(i,str) and (i in j)):
             count_dic[i]=count_dic[i]+1
          
        elif(set(i).issubset(set(j))):
          count_dic[i]=count_dic[i]+1;
    
    return list(count_dic.items())
"""

def SON_phase2(x,candidate_itemsets):
    x=list(x)

    count_dic=defaultdict(lambda : 0);

     
    for i in candidate_itemsets:
      for j in x:          
        if(set(i).issubset(set(j))):
          count_dic[i]=count_dic[i]+1;
    
    return list(count_dic.items())


frequent_itemsets=srdd.mapPartitions(lambda x : SON_phase2(x,candidate_itemsets)).reduceByKey(lambda x,y : x+y).filter(lambda x : x[1]>=threshold).map(lambda x : x[0]).collect()


oneset=[]
result=defaultdict(lambda : [])
for i in frequent_itemsets:
  if(len(i)==1):
    i=str(i)
    oneset.append(i[0:-2]+i[-1]);
    #oneset.append("("+i+")")
  else:
    result[len(i)].append(str(i))

f.write("Frequent Itemsets:");
f.write("\n")

if(oneset):
  oneset=sorted(oneset)
  temp=",".join(oneset)
  f.write(str(temp))
  f.write("\n\n")
temp=list(result.keys());
temp.sort()

for i in temp[0:-1]:
  temp_str=",".join(sorted(result[i]));
  f.write(str(temp_str))
  f.write("\n\n")

temp_str=",".join(sorted(result[temp[-1]]));
f.write(str(temp_str))    
f.close()

end=time.time()
print("Duration:",end-start);