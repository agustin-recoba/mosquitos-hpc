# Ejecutable

(archivo de ventas) -> (cambios de tendencia, totales interesantes, max-min-avg de las medidas, conteos interesantes)

## Idea de trabajo

### Regresion

cambios de tendencia
Si lo hacemos a nivel categoría:

- Puede haber ruido en el dato: hay productos de rangos de precio distintos.

Tendencias hipótesis:

- Suba de precio en el periódo de invasión.
- Aumento sustantivo en el volumen total vendido.
- Escasez luego del boom.

Control:

- Meses del año anterior
- Categoría de refrescos

### Totales y max-min-avg

Cuentas básicas en el reducer

### Conteos interesantes

Ver cómo meterlo

## Variaciones para probar

### Metodo de JOIN

Para mostrar que probamos varios modelos de la arqui. Map-Reduce.

Variando clase Driver.

Pendiente:

- Implementar otro tipo de JOIN. Lo podemos dejar para más adelante, tenemos un join que anda (y ya sabemos que es el óptimo).

### Algoritmo de regresión

Para mostrar que probamos varias soluciones para el problema. Una sencilla y otra no.

Variando clase Driver.

Pendiente:

- Implementar Reg. Segmentada
  - Debería ser sencillo con lo que ya tenemos. Se hace en el reducer luego de que el mapper te agrupó por la clave que interesaba.
- Implementar PELT
  - Meterle cabeza a adaptar el algoritmo a MapReduce.

### Base de ventas

Para mostrar si escala en tamaño.

Variando el parámetro /ventas_dataset_i/

Pendiente:

- Extraer las bases más grandes de Scann

### Cantidad de nodos

Para mostrar el speedup.

Levantando más nodos en docker y conectando más máquinas o GCLOUD.

Pendiente:

- Probar nuestros Job implementados en GCLOUD.
