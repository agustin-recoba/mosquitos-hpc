# Correr esto en dataproc de Google Cloud

Copiar los datos del bucket
$ hadoop distcp gs://bucket-hpc/data/ /

En el archivo fuente hay que cambiar el path de los datos:
Cambiar en CacheHdfs:
De:
hdfs://hadoop-master:9000/data
A:
hdfs://example-cluster-m/data

# Pruebas de escalamiento

Con estas flags se limita la cantidad de tareas map y reduce:
-Dmapreduce.job.running.map.limit
