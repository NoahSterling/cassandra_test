from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import random
import time

cluster = Cluster(["172.17.0.2"])
session = cluster.connect('user2')

print('Hello, Lily!')
result = session.execute("select * from user2.rec where name='Lily'")

users = ['Noah', 'John', 'Eve', 'Nancy']
books = ['Sea', 'Lady', 'Superman', 'You', 'Snake', 'Ngiht']

var = 1
while var == 1 :
    rows = session.execute('select * from user2.rec;')
    for i in rows:
        for j in result:
            if i.name != 'Lily' and i.book == j.book and abs(int(i.level)-int(j.level))<=1:
                print("%s may become your friend! You agree on %s!"% (i.name, i.book))
    time.sleep(2)            
                        
