#!/bin/bash

# add the data into hdfs

# create input directory on HDFS
hadoop fs -mkdir -p /data

# put input files to HDFS
hdfs dfs -put ./data/* /data/