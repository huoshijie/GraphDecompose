#!/usr/bin/env bash

begin_time=`date +%s`
echo "${begin_time}"

DataSet=$1
outDir=$2
iteration=$3
index=$4

echo "DataSet-----${DataSet}"
echo "outDir-----${outDir}"

in_put_dir="hdfs://10.1.14.20:9000/graph/SmallData/${DataSet}/network.dat"
real_partition_path="hdfs://10.1.14.20:9000/graph/SmallData/${DataSet}/community.dat"

out_put_dir="hdfs://10.1.14.20:9000/graph/output/${outDir}"

glb_jar="./target/GraphDecompose-1.0-SNAPSHOT-jar-with-dependencies.jar"

/home/hadoop/program/spark-2.0.0-bin-hadoop2.4/bin/spark-submit  \
                    --driver-memory 10g \
                    --executor-memory 25g \
                    --num-executors 5 \
                    --conf "spark.yarn.executor.memoryOverhead=3g" \
                    --conf "spark.executor.cores=1" \
                    --conf "spark.executor.memory=25g" \
                    --class GraphDecompose_GraphPartition_CliqueBK \
                    ${glb_jar} \
                    --in_put_dir "${in_put_dir}" \
                    --out_put_dir "${out_put_dir}" \
                    --iteration $iteration \
                    --index $index \
                    --real_partition_path $real_partition_path

if [ $? -ne 0 ]; then
    exit 1
fi

end_time=`date +%s`
echo "total time:"$(($end_time - $begin_time))
