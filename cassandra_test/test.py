from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

from time import time

cluster = Cluster(["172.17.0.2"])
session = cluster.connect('user2')
session.execute("TRUNCATE user2.test;")

start = time()
data_cnt = 100000
bulk_cnt = 100
for i in range((int)(data_cnt/bulk_cnt)):
    insert_user = session.prepare("insert into user2.test (name, age, city) values (?, ?, 'Austin')")
    batch = BatchStatement()
    for j in range(bulk_cnt):
        batch.add(insert_user, ("Jones"+str(i*bulk_cnt+j), j))
    session.execute(batch)

end = time()
interval = end-start
print ("time:", interval, "through ops/per second:", data_cnt/interval)

start2 = time()
result = session.execute("select * from user2.test where name='Jones54'")

for x in result:
    print (x.age, x.name, x.city)

end2 = time() - start2
print("time:",end2)
