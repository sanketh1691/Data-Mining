import pyspark
import sys
import re
import time
import math as ma
import csv
import os
from itertools import combinations as comb
from collections import defaultdict,Counter
from functools import reduce
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import StringType,StructType,StructField
from graphframes import GraphFrame as GF

import time

os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

start=time.time()

configuration=pyspark.SparkConf().setAppName('task1').setMaster('local[3]')

sci=pyspark.SparkContext(conf=configuration)

spark=SQLContext(sci)

sci.setLogLevel("ERROR")
sci.setLogLevel("OFF")


ub_rdd=sci.textFile(sys.argv[2])

ubrd=ub_rdd.map(lambda x : tuple(x.split(",")))

head=ubrd.take(1)[0]
ubr=ubrd.filter(lambda x : x!=head)

uids=ubr.map(lambda x : x[0]).distinct().sortBy(lambda x : x).collect()

user_bus=ubr.groupByKey().map(lambda x : (x[0],list(set(x[1])))).collectAsMap()

user_pairs=list(comb(uids,2))

nodes=[]
directed_edges=[]
threshold=int(sys.argv[1])
for data in user_pairs:
  x=data[0]
  y=data[1]
 
  if(len(set(user_bus[x]) & set(user_bus[y]))>=threshold):
    nodes.append((x,))
    nodes.append((y,))
    directed_edges.append((x,y))
    directed_edges.append((y,x))
    
nodes=list(set(nodes))
#sch=StructType([StructField('vertices',StringType(),True)])
nodes_df=spark.createDataFrame(nodes,["id"])  
edges_df=spark.createDataFrame(directed_edges,["src","dst"])


number_iterations=5
g=GF(nodes_df,edges_df)
graph=g.labelPropagation(maxIter=number_iterations)

g_rdd=graph.rdd

grdd=g_rdd.map(lambda x : (x[1],x[0])).groupByKey().map(lambda x : sorted(list(x[1]))).sortBy(lambda x : (len(x),x[0]))

final_data=grdd.collect()

file = open(sys.argv[3],"w");

for i in final_data[0:-1]:
    file.write(str(i)[1:-1])
    file.write("\n");
file.write(str(final_data[-1])[1:-1])    
    
file.close()    

end=time.time()

print(end-start)




