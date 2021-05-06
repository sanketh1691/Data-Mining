import pyspark
import sys
import json
import re
import time
import math as ma
import queue as q
from itertools import combinations as comb
from collections import defaultdict,Counter
import copy

start=time.time()

sc=pyspark.SparkContext('local[*]')

sc.setLogLevel("ERROR")
sc.setLogLevel("OFF")

user_state=sc.textFile(sys.argv[2]).map(lambda x : tuple(x.split(","))).map(lambda x : tuple(x))

head=user_state.take(1)[0]
usrdd=user_state.filter(lambda x : x!=head)

#################################################################################################################
# BETWEENNESS

def betweenness(adj_vertices):
    ed=defaultdict(lambda : 0);

    for point in adj_vertices:
      
        unseen=q.Queue();
        seen=[]
        unseen.put(point);

        parents_dict=defaultdict(lambda : set())
        s_paths=defaultdict(lambda : 0)
        s_paths[point]=1;
        levels_dict={}
        levels_dict[point]=0;

        while (unseen.empty() == False):

          index_node=unseen.get()
          #appending the seen element in seen list
          seen.append(index_node);

          for side in adj_vertices[index_node]:

            if side not in levels_dict:

                levels_dict[side]=levels_dict[index_node]+1  

                #if not s_paths[side]:
                 # s_paths[side]=s_paths[index_node];

                #elif s_paths[side] and (levels_dict[side]<levels_dict[index_node] or levels_dict[side]>levels_dict[index_node]):
                 # s_paths[side]=s_paths[side]+s_paths[index_node];
               
                s_paths[side]=1;
                  
                if side not in unseen.queue:
                   unseen.put(side)

                parents_dict[side].add(index_node)  

            else:
                
                if (levels_dict[side]-levels_dict[index_node]==1):
                    #continue;

                    parents_dict[side].add(index_node);
                   #parents_dict[index_node].add(side);
                    s_paths[side]=s_paths[side]+1
                   #continue;


        node_credits=defaultdict(lambda : 1);
        edge_btns={}
        
        btns=sorted(levels_dict.keys(),key=lambda x : levels_dict[x], reverse=True)
        
        #rev_vis=[]
        seen.reverse()
        #c=0;
        for node in btns:
          #c=c+1;
          if(node!=point):
                
              t_proportion=0;
              for i in parents_dict[node]:
                t_proportion=t_proportion+s_paths[i];
              #dc=0
              for i in list(parents_dict[node]):
                #dc=dc+1;
                temp=tuple(sorted((i,node)));

                edge_btns[temp] = float(node_credits[node]*s_paths[i]) / float(t_proportion)

                node_credits[i]=node_credits[i]+edge_btns[temp];
            
        for lines in edge_btns:
            #if lines in edges:
        
               ed[lines]=ed[lines]+(edge_btns[lines]/2) 
            
    edg=sorted(ed.items(), key=lambda x : (-x[1],x[0]))    
    return edg; 


#################################################################################################################
# COMMUNITY DIVISION

def community_division(adj_vertices):
    vert=set();
    for i in adj_vertices.keys():
      vert.add(i);
    comm=[];
    while len(vert)!=0:
        start=list(vert)[0];
        unseen=q.Queue()
        unseen.put(start);
        seen=[];    
        #print(len(vert))
        while (unseen.empty() == False):
            node=unseen.get()
            seen.append(node);
            #print(len(unseen.queue))
            for j in adj_vertices[node]:
              if j not in unseen.queue and j not in seen:
                  unseen.put(j);
              else: 
                continue;   
        comm.append(sorted(seen));
        vert.difference_update(set(seen))
    
    return comm;   
          
#####################################################################################################################
# MODULARITY       

def modularity_value(m,comm,adj_ver):
   
   modularity_val=0;
   for com in comm:
     p_mod=0;
     for n1 in com:
       for n2 in com:
          t1=len(adj_ver[n1])
          t2=len(adj_ver[n2])
          #t1=degree_matrix[n1]
          #t2=degree_matrix[n2]  
          
          if (tuple(sorted((n1,n2))) in edges): 
             val=1
          else:
             val=0
          temp= (val-((t1*t2)/(2*m)))
          #temp=(adj_matrix[(n1,n2)]-((t1*t2)/(2*m)))
          modularity_val=modularity_val+temp;
   
   return modularity_val/(2*m);

   
#modularity_value(m_value,communities,adj_vertices)
########################################################################################################################
# MODULARITY FOR BI-PARTITE GRAPH

def modularity_value_2(m,comm,adj_ver):
   
   modularity_val=0;
   for com in comm:
     p_mod=0;
     for n1 in com:
       for n2 in com:
          t1=len(adj_ver[n1])
          t2=len(adj_ver[n2])
          #t1=degree_matrix[n1]
          #t2=degree_matrix[n2]  
          if (tuple(sorted((n1,n2))) in user_state): 
             val=1
          else:
            val=0
          temp= (val-((t1*t2)/m))
          #temp=(adj_matrix[(n1,n2)]-((t1*t2)/m))
          modularity_val=modularity_val+temp;
   
   return modularity_val/(2*m);    
    
    

if(int(sys.argv[1])==2):
    
    users=usrdd.map(lambda x : x[0]).distinct().sortBy(lambda x : x).collect()
    user_pairs=list(comb(users,2))

    u_s=usrdd.groupByKey().map(lambda x : (x[0],list(set(x[1])))).collectAsMap()
    
    #vertices=defaultdict(lambda x : [])
    edges=defaultdict(lambda : 0)

    nodes=set()
    for i in user_pairs:
      x=i[0]
      y=i[1]
      num=len(set(u_s[x]).intersection(set(u_s[y])))
      den=len(set(u_s[x]).union(set(u_s[y]))) 
      jac=num/den

      if(jac>=0.5):
        edges[i]=0;
        nodes.add(x);
        nodes.add(y);

    adj_vertices=defaultdict(lambda : set())

    for ed in edges:
      adj_vertices[ed[0]].add(ed[1])
      adj_vertices[ed[1]].add(ed[0])
        
        
    edg=betweenness(adj_vertices)
        

    file=open(sys.argv[3],"w");

    for i in edg:
      file.write(str(i)[1:-1])
      file.write("\n")
    file.close()        

    mod_edges={}
    for i in edg:
      mod_edges[i]=0; #Using modularity Edges (Used for calculating modularity)    
    
    adj_matrix=defaultdict(lambda : 0);
       

    #degree_matrix=defaultdict(lambda x : 0);

    #for deg in adj_vertices:
    #  degree_matrix[deg]=len(adj_vertices[deg])

    original_graph=copy.deepcopy(adj_vertices)

    m_value=len(mod_edges) # Using mod edges here        
    
    communities=community_division(adj_vertices)
    max_modu=modularity_value(m_value,communities,adj_vertices)

    num_edges=m_value;

    while num_edges>0:
           h_edge_val=edg[0]
           h_val=h_edge_val[1]
           h_edge=h_edge_val[0]
           temp=h_edge
           #for val in edg:
              #temp=val[0];
              #if(val[1]==h_val):
                #if (h_edge[0] in adj_vertices) and (h_edge[1] in adj_vertices[h_edge[0]]):
           adj_vertices[temp[0]].remove(temp[1]);
                #if (h_edge[1] in adj_vertices) and (h_edge[0] in adj_vertices[h_edge[1]]):   
           adj_vertices[temp[1]].remove(temp[0]);
           num_edges=num_edges-1;


           comm=community_division(adj_vertices);
           # This is where the community division happens
           modu=modularity_value(m_value,comm,original_graph)
           # This is where we calculate the modularity

           if(modu>=max_modu):
             max_modu=modu;
             communities=comm;

           edg=betweenness(adj_vertices);  
           #num_edges=len(edg);

             
                
    final_value=sorted(communities,key=lambda x : (len(x),x))
            
    fi=open(sys.argv[4],"w");
    
    for j in final_value:
      fi.write(str(j)[1:-1]);
      fi.write("\n")
    fi.close()  

    
if(int(sys.argv[1])==1):   
    
    print("Fill it")
    user_state=usrdd.map(lambda x : tuple(sorted(x))).collect()   
    
    adj_vertices=defaultdict(lambda : set())
    node_users=set();
    node_states=set();
    nodes=set();
    
    for us in user_state:
      adj_vertices[us[0]].add(us[1])
      adj_vertices[us[1]].add(us[0])
      nodes.add(us[0])
      nodes.add(us[1])

    
    edg=betweenness(adj_vertices)
    
    
    fil=open(sys.argv[3],"w");

    for i in edg:
      fil.write(str(i)[1:-1])
      fil.write("\n")
    fil.close()
   
    mod_edges={}
    for i in edg:
      mod_edges[i]=0;
    
    adj_matrix=defaultdict(lambda : 0);


    original_graph=copy.deepcopy(adj_vertices)
    
    m_value=len(mod_edges) # Using mod edges here
    
    communities=community_division(adj_vertices)
    max_modu=modularity_value_2(m_value,communities,adj_vertices)
    
    num_edges=m_value;

    while num_edges>0:
           h_edge_val=edg[0]
           h_val=h_edge_val[1]
           h_edge=h_edge_val[0]

           for val in edg:
              temp=val[0];
              if(val[1]==h_val):
                #if (h_edge[0] in adj_vertices) and (h_edge[1] in adj_vertices[h_edge[0]]):
                  adj_vertices[temp[0]].remove(temp[1]);
                #if (h_edge[1] in adj_vertices) and (h_edge[0] in adj_vertices[h_edge[1]]):   
                  adj_vertices[temp[1]].remove(temp[0]);
                  num_edges=num_edges-1;


           comm=community_division(adj_vertices);
           # This is where the community division happens
           modu=modularity_value_2(m_value,comm,original_graph)
           # This is where we calculate the modularity

           if(modu>=max_modu):
             max_modu=modu;
             communities=comm;

           edg=betweenness(adj_vertices);  
           #num_edges=len(edg);

    final_value=sorted(communities,key=lambda x : (len(x),x))
    
    
    fii=open(sys.argv[4],"w");
    
    for i in final_value:
      fii.write(str(i)[1:-1]);
      fii.write("\n")
    fii.close()      

    
    
    
end=time.time()
print(end-start)
