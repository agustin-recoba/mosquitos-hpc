#!/bin/bash

# the default node number is 3
N=${1:-3}

# master container
echo "remove hadoop-master container..."
sudo docker rm -f hadoop-master &> /dev/null


# slave container
i=1
while [ $i -lt $N ]
do
	sudo docker rm -f hadoop-slave$i &> /dev/null
	i=$(( $i + 1 ))
done 