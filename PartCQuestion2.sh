#!/bin/sh

start_spark_job() {
    $SPARK_HOME/bin/spark-submit --conf spark.driver.memory=1g \
                                 --conf spark.eventLog.enabled=true \
                                 --conf spark.eventLog.dir=hdfs://10.254.0.146/spark/history \
                                 --conf spark.executor.memory=1g \
                                 --conf spark.executor.cores=4 \
                                 --conf spark.task.cpus=1 \
                                 --master spark://10.254.0.146:7077 \
                                 --class PageRankPartC2 \
                                 /home/ubuntu/rohit/PageRankSparkApp/target/scala-2.11/page-rank-group-23_2.11-1.0.jar
}

echo "Clearing cache"
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"

io_read_before=`iostat -d | tail -2 | head -1 | tr -s ' ' | cut -d ' ' -f5`
io_write_before=`iostat -d | tail -2 | head -1 | tr -s ' ' | cut -d ' ' -f6`

net_recv_before=`netstat -i | grep eth0 | tr -s ' ' | cut -d ' ' -f4`
net_send_before=`netstat -i | grep eth0 | tr -s ' ' | cut -d ' ' -f8`

echo "Running Job"
start_spark_job

io_read_after=`iostat -d | tail -2 | head -1 | tr -s ' ' | cut -d ' ' -f5`
io_write_after=`iostat -d | tail -2 | head -1 | tr -s ' ' | cut -d ' ' -f6`

net_recv_after=`netstat -i | grep eth0 | tr -s ' ' | cut -d ' ' -f4`
net_send_after=`netstat -i | grep eth0 | tr -s ' ' | cut -d ' ' -f8`

io_read=$(( io_read_after - io_read_before ))
io_write=$(( io_write_after - io_write_before ))

net_recv=$(( net_recv_after - net_rect_before ))
net_send=$(( net_send_after - net_send_before ))

echo "IO_read="$io_read" IO_write="$io_write
echo "Net_recv="$net_recv" Net_send="$net_send
