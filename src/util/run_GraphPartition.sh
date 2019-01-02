#!/usr/bin/env bash

begin_time=`date +%s`
echo "${begin_time}"

DataSet=$1
outDir=$2
echo "DataSet-----${DataSet}"
echo "outDir-----${outDir}"

in_put_dir="hdfs://10.1.14.20:9000/graph/${DataSet}"

out_put_dir="hdfs://10.1.14.20:9000/graph/output/${outDir}"
 
glb_jar="./target/GraphDecompose-1.0-SNAPSHOT-jar-with-dependencies.jar"

/home/hadoop/program/spark-2.0.0-bin-hadoop2.4/bin/spark-submit  \
                    --driver-memory 10g \
                    --executor-memory 20g \
                    --conf "spark.driver.maxResultSize=1000" \
                    --conf "spark.dynamicAllocation.enabled=true" \
                    --conf "spark.dynamicAllocation.minExecutors=5" \
                    --conf "spark.dynamicAllocation.maxExecutors=10" \
                    --conf "spark.yarn.executor.memoryOverhead=3g" \
                    --conf "spark.executor.cores=2" \
                    --conf "spark.executor.memory=20g" \
                    --class GraphPartitionChange20180628 \
                    ${glb_jar} \
                    --in_put_dir "${in_put_dir}" \
                    --out_put_dir "${out_put_dir}"

if [ $? -ne 0 ]; then
    exit 1
fi

end_time=`date +%s`
echo "total time:"$(($end_time - $begin_time))
