#!/bin/sh
workspace="/user/guanggao/car_similar/v4/pregraph/nodes_pairs/part-*"
conf=$workspace

spark="/data/sysdir/spark-2.0.2-with-hive/bin/spark-submit"
exec="--queue q_guanggao.q_adalg 
--num-executors 200
--driver-memory 20G
--executor-memory 10G
--executor-cores 8
--master yarn 
--queue=q_guanggao.q_adalg
--conf spark.shuffle.memoryFraction=0.6
--conf spark.storage.memoryFraction=0.7
--conf spark.memory.storageFaction=0.5
--class com.autohome.algo.simrank.Simrank
/data/home/guanggao/semon/JAR/Simrank/com.autohome.algo.simrank.jar $conf"
echo $exec

#--conf spark.default.parallelism=800
#--conf spark.kryoserializer.buffer.max=1g
$spark $exec

