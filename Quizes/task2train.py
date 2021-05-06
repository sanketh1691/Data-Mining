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
train_rdd=sc.textFile(sys.argv[1]).map(lambda x : json.loads(x))

#Stopwords
stopwords=[]
file= open(sys.argv[3],"r")
for i in file:
  stopwords.append(i.rstrip())

#Parsing the strings to get words for item profiles
def parsing(data):
   x=data[1]
   x=x.lower();
   x=re.sub(r'[^\w\s]|[_|\d]','',x)
   temp=x.split()
   temp=[(i,data[0]) for i in temp if i not in stopwords]
   return temp

#Counting Number of Records
bus_rdd=train_rdd.map(lambda x : (x['business_id'],x['text'])).reduceByKey(lambda x,y : x+y)
total_records=bus_rdd.count()

#Total Number of words in all the documents(Items) together
busrdd=bus_rdd.map(parsing)
busrdd_2=busrdd.flatMap(lambda x : x)
total_words=busrdd_2.count()

#Calculating the counts of number of documents does each word occurs and number of times it occurs in all the docs and IDF
def frequency_func(x):
   x=list(x)
   total_word_count=len(x)
   total_word_doc_count=len(set(x))
   IDF=(m.log10(total_word_doc_count/total_records)/m.log10(2))
   return (total_word_count,IDF)

#Words with their count in all the documents together with duplicates and IDF
words_counts_dic=busrdd_2.groupByKey().mapValues(frequency_func).filter(lambda x : x[1][0]>=(0.0001*total_words)).collectAsMap()


#Calculating Term Frequenct for every word
def term_frequency(x):
    x=list(x)
    x=[i for i in x if i in list(words_counts_dic.keys())]
    term_freq=Counter(x)
    max_freq=max(list(term_freq.values()))
    for k in term_freq.keys():
      term_freq[k]=(term_freq[k]/max_freq)*words_counts_dic[k][1]
    temp=sorted(term_freq.items(),key=lambda x : x[1], reverse=True)
    
    return list(zip(*temp))[0][0:200]

#Getting the Item profiles with highest TF-IDF
item_profiles_rdd=busrdd_2.map(lambda x : (x[1],x[0])).groupByKey().mapValues(term_frequency).persist()

#Getting item profiles as dictionary
item_profiles=item_profiles_rdd.collectAsMap()


#Getting User profiles, taking union of item profiles that the user likes

def user_profiles(x):
   x=list(set(x))
   temp=set()
   for i in x:
     temp=temp.union(set(item_profiles[i]))
   return tuple(temp)


#Getting User Profiles
user_profiles_dic=train_rdd.filter(lambda x : x["user_id"]!=None and x["business_id"]!=None).map(lambda x : (x["user_id"],x["business_id"])).groupByKey().mapValues(user_profiles).collectAsMap()

file=open(sys.argv[2],"wb")
pickle.dump([item_profiles,user_profiles_dic],file,protocol=pickle.HIGHEST_PROTOCOL)
file.close()

end=time.time()
print("---------------",end-start,"-----------------")
