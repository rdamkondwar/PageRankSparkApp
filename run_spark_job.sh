#!/bin/sh

start_spark_job() {
    $SPARK_HOME/bin/spark-submit --conf spark.driver.memory=1g \
                                 --conf spark.eventLog.enabled=true \
                                 --conf spark.eventLog.dir=hdfs://10.254.0.146/spark/history \
                                 --conf spark.executor.memory=1g \
                                 --conf spark.executor.cores=4 \
                                 --conf spark.task.cpus=1 \
                                 --master spark://10.254.0.146:7077 \
                                 --class PageRankPartC1 \
                                 /home/ubuntu/rohit/PageRankSparkApp/target/scala-2.11/page-rank-group-23_2.11-1.0.jar
}

start_spark_job
