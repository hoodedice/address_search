start the containers:
```
HADOOP_HOME=/opt/hadoop docker-compose up -d  
```

scale the containers:
```
docker-compose scale datanode=4
```

plop parquent into hadoop:
```
docker cp full_address_data.parquet hdfs-namenode-1:/opt/hadoop
docker exec -it hdfs-namenode-1 /bin/bash
hdfs dfs -mkdir -p /user/root
hdfs dfs -put full_address_data.parquet /user/root
```

plop the jar file:
```

```
