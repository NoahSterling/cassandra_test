from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import random
import time

cluster = Cluster(["172.17.0.2"])
session = cluster.connect('user2')

users = ['Lily', 'Noah', 'John', 'Eve', 'Nancy']
books = ['Sea', 'Lady', 'Superman', 'You', 'Snake', 'Ngiht']

usersList = random.sample(range(0,5),5);

for i in usersList:
    insert_user = session.prepare("insert into user2.rec (name, book, level) values (?, ?, ?)")
    booksList = random.sample(range(0,6),6);
    batch = BatchStatement()
    for j in booksList:
        k = str(random.randint(1, 5))
        batch.add(insert_user, (users[i], books[j], k))
    session.execute(batch)
    time.sleep(random.randint(0, 5))

