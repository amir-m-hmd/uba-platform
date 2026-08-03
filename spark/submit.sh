#!/bin/bash

echo "Waiting for Redpanda, Postgres, ClickHouse, and Spark Master..."
sleep 7


spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  --driver-memory 512m \
  --executor-memory 512m \
  --conf spark.executor.memoryOverhead=256m \
  --conf spark.driver.memoryOverhead=256m \
  --conf spark.sql.shuffle.partitions=2 \
  /app/spark_streaming.py