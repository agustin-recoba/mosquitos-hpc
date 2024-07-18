### 0. Clone the repo

```
$ git clone https://github.com/agustin-recoba/hadoop-cluster-docker.git
```

### 1. Create docker network

```
$ sudo docker network create --driver=bridge hadoop
```

### 2. Start containers

```
$ cd hadoop-cluster-docker
$ sudo ./start-container.sh $QTY_NODES
```

### 3. Run test wordcount

```
$ ./run-wordcount.sh
```

**output**

```
input file1.txt:
Hello Hadoop

input file2.txt:
Hello Docker

wordcount output:
Docker    1
Hadoop    1
Hello    2
```

### Considerations

- /data will be binded to the master node and added to hdfs automaticaly
- /src will be binded to the master node for development propourses
- YARN and DFS will be started automatically
