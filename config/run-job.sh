#!/bin/bash

driverClass=$1
input=$2
output=$3

# run wordcount 
hadoop jar /root/src/mosquito/target/mosquito-1.jar fing.hpc.$driverClass $input $output


# print the output of wordcount
echo -e "\n Resultado:"
hdfs dfs -cat $output/part-r-00000 | head

