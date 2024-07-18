#!/bin/bash

# the default node number is 3
N=${1:-3}

if [ $# = 0 ]
then
	echo "Please specify the node number of hadoop cluster!"
	exit 1
fi

# change slaves file
i=1
rm config/slaves
while [ $i -lt $N ]
do
	echo "hadoop-slave$i" >> config/slaves
	((i++))
done 

echo ""

sudo docker pull ubuntu:14.04

echo -e "\nbuild docker hadoop image\n"

# rebuild kiwenlau/hadoop image
sudo docker build -t hpc:hadoop .

echo ""

# start hadoop master container
sudo docker rm -f hadoop-master &> /dev/null
echo "start hadoop-master container..."
sudo docker run -itd \
                --net=hadoop \
                -p 50070:50070 \
                -p 8088:8088 \
				--mount type=bind,source="$(pwd)"/data,target=/root/data \
				--mount type=bind,source="$(pwd)"/src,target=/root/src \
                --name hadoop-master \
                --hostname hadoop-master \
                hpc:hadoop &> /dev/null


# start hadoop slave container
i=1
while [ $i -lt $N ]
do
	sudo docker rm -f hadoop-slave$i &> /dev/null
	echo "start hadoop-slave$i container..."
	sudo docker run -itd \
	                --net=hadoop \
	                --name hadoop-slave$i \
	                --hostname hadoop-slave$i \
	                hpc:hadoop &> /dev/null
	i=$(( $i + 1 ))
done 

sudo docker exec hadoop-master bash /root/start-hadoop.sh

sudo docker exec hadoop-master bash /root/dfs-put-data.sh