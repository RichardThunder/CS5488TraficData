# Big Data analysis for Hong kong traffic data

## general instruction

- this project aims to practice big data mechnism, using hadoop hdfs to store data, spark to compute, yarn to manage resource

- analysis traffic data

- benchamrk computation in different tool, such as parallel spark, hive sql, serialize pandas, 

## preprocessing data part
1_xml_to_csv_py
this code convert xml files to parquet in hdfs, remove invalid data, combine detector data with geolocation data

## analysis part
todo

## benchmark part
./benchmark

on doing

use spark, hive pandas to test execution time, monitor and record

the data dir is in hdfs:///traffic_data_partitioned/, from hdfs:///traffic_data_partitioned/202409 to hdfs:///traffic_data_partitioned/202508, each month about 1gb, 

## system info

ubuntu 24, run on 16cores, 32vcores, 64gb ram, 

hadoop-namenode run on vm 
hadoop-datanode1,2,3 run on vm 15gb 8cores

to connect hadoop-namenode: ssh hadoop@hadoop-namenode

