/bin/bash /usr/local/hadoop/sbin/stop-dfs.sh

rm -rf /tmp/*
hdfs namenode -format

/bin/bash /usr/local/hadoop/sbin/start-dfs.sh

sleep 5

hdfs dfs -mkdir /input
hdfs dfs -mkdir /input/queries

hadoop fs -put ./data/queries /input
hdfs dfs -put ./data/citeseer.unsafe.graph /input/
hdfs dfs -put ./data/patent.unsafe.graph /input/
hdfs dfs -put ./data/youtube.unsafe.graph /input/
hdfs dfs -put ./data/mico.unsafe.graph /input/
hdfs dfs -put ./data/citeseer-single-label.graph /input/
#hdfs dfs -put ./data/citeseer-single-label.graph /input/
#hdfs dfs -put ./data/queries/Q1-citeseer /input/queries/
#hdfs dfs -put ./data/queries/Q5-citeseer /input/queries/
#hdfs dfs -put ./data/queries/Q5u /input/queries/
