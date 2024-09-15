# mosquitos-hpc: análisis de ventas de repelentes con relación a las invasiones de mosquitos de 2024

Proyecto Java para Hadoop MapReduce que permite ejecutar algoritmos de detección de tendencias sobre series temporales, aplicados a datos
de ventas de productos relacionados con control de plagas (repelentes e insecticidas).

Trabajo realizado en el marco del proyecto final de curso de la asignatura "Computación de Alta Performance" de la carrera Ingeniería en Computación de la FING, UDELAR.

## Join de tablas

Implementamos varias estrategias de Join para la combinación de datos de venta con datos de cada producto y punto de venta.
Se pueden probar con los driver `ReduceJoinDriver` y `TestHdfsHashJoinDriver`.

## Algoritmos de detección de tendencias

También implementamos varios algoritmos de detección de tendencias, como Regresión Segmentada, PELT y PELT distribuido.

- Para ejecutar el algoritmo de Regresión Segmentada, se puede usar el driver `SegmentedRegressionDriver`.
- Para ejecutar el algoritmo de PELT, se puede usar el driver `SimplePELTDriver`.
- Para ejecutar el algoritmo de PELT distribuido, se puede usar el driver `DealPELTDriver`.

## Informe completo

Para más detalles, se puede consultar el informe completo en el archivo `Informe final.pdf`.
