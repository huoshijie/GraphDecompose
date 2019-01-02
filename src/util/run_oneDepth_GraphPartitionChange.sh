#!/usr/bin/env bash

begin_time=`date +%s`
echo "${begin_time}"

DataSet=$1
outDir=$2
echo "DataSet-----${DataSet}"
echo "outDir-----${outDir}"

in_put_dir="hdfs://10.1.14.20:9000/graph/${DataSet}"

out_put_dir="hdfs://10.1.14.20:9000/graph/One_output/${outDir}"

glb_jar="./1.0-SNAPSHOT-1.0-SNAPSHOT.jar"

/home/hadoop/program/spark-2.0.0-bin-hadoop2.4/bin/spark-submit \
                    --driver-memory 40g \
                    --executor-memory 20g \
                    --conf "spark.driver.maxResultSize=80g" \
                    --conf "spark.dynamicAllocation.enabled=true" \
                    --conf "spark.dynamicAllocation.minExecutors=5" \
                    --conf "spark.dynamicAllocation.maxExecutors=5" \
                    --conf "spark.yarn.executor.memoryOverhead=3g" \
                    --conf "spark.executor.cores=1" \
                    --conf "spark.executor.memory=25g" \
                    --conf "spark.speculation.interval=100" \
                    --class GraphDecompose_GraphPartition_CliqueBK \
                    ${glb_jar} \
                    --in_put_dir "${in_put_dir}" \
                    --out_put_dir "${out_put_dir}"

if [ $? -ne 0 ]; then
    exit 1
fi

end_time=`date +%s`
echo "total time:"$(($end_time - $begin_time))
