#!/bin/bash

driverClass=$1
input=$2
max_reducers=$3
max_mappers=$4
output=$5


echo -e "\n Cantidad de mapers simultaneos máximos: $max_mappers"
echo -e "\n Cantidad de reducers simultaneos máximos: $max_reducers"

# run wordcount 
hadoop jar /root/src/mosquito/target/mosquito-1.jar fing.hpc.$driverClass -Dmapreduce.job.running.map.limit=$max_mappers -Dmapreduce.job.running.reduce.limit=$max_reducers $input $output


# print the output of wordcount
echo -e "\n Resultado:"
hdfs dfs -cat $output/part-r-00000 | head

