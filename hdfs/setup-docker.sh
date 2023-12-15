# start the containers:
HADOOP_HOME=/opt/hadoop docker-compose up -d  

# scale the containers:
HADOOP_HOME=/opt/hadoop docker-compose scale datanode=11

# plop parquent into hadoop:
cd ../server/data/archive
pwd
docker cp full_address_data.csv hdfs-namenode-1:/opt/hadoop
# docker exec hdfs-namenode-1 /bin/bash
docker exec hdfs-namenode-1 hdfs dfs -mkdir -p /user/root
docker exec hdfs-namenode-1 hdfs dfs -put full_address_data.csv /user/root

# plop the jar file:
cd ../../../hadoop-java/out/artifacts/hadoop_java_jar
pwd
docker cp hadoop-java.jar hdfs-namenode-1:/opt/hadoop

# copy output file later
# hdfs dfs -get /user/root/test.csv
# docker cp hdfs-namenode-1:/opt/hadoop/test.csv test.csv
