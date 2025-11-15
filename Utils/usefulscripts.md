```shell
hdfs dfs -ls

hdfs dfs -du -s  -h traffic_checkpoints
33.0 G  33.0 G  traffic_checkpoints

hdfs dfs -put

hdfs dfs -get

nohup spark-submit \      
    --master "local[*]" \
    --driver-memory 8g \
    --executor-memory 8g \
    1.py dataset/202508 > 1.out 2>&1 &

spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 3g \
    --executor-cores 3 \
    --num-executors  5\
    --packages com.databricks:spark-xml_2.12:0.17.0 \
    /home/richard/project/BDA/pyspark/1.py /home/richard/project/BDA/pyspark/dataset/subset
```

```shell
spark-submit \
    --master "local[*]" \
    --driver-memory 4g \
    --executor-memory 16g \
    --executor-cores 4 \
    --num-executors  3\
    --packages com.databricks:spark-xml_2.12:0.17.0 \
    1_xml_to_csv.py file:///home/richard/project/BDA/pyspark/dataset/202508
```

exit safe mode

```shell
hdfs dfsadmin -safemode leave
```

```shell
# run test
nohup spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 3g \
    --executor-cores 3 \
    --num-executors  5\
    --packages com.databricks:spark-xml_2.12:0.17.0 \
    benchmark/example_usage.py >benchmark/test.out 2>&1 &
```
