#!/bin/sh
workspace="/user/data/data.txt"
conf=$workspace

spark="/spark-2.0.2-with-hive/bin/spark-submit"
exec="--queue simrank
--num-executors 200
--driver-memory 20G
--executor-memory 10G
--executor-cores 8
--master yarn 
--queue=simrank
--conf spark.shuffle.memoryFraction=0.6
--conf spark.storage.memoryFraction=0.7
--conf spark.memory.storageFaction=0.5
--class com.autohome.algo.simrank.Simrank
/JAR/Simrank/simrank.jar $conf"
echo $exec

#--conf spark.default.parallelism=800
#--conf spark.kryoserializer.buffer.max=1g
$spark $exec

