package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.ToolRunner;

public class DealPELTDriver extends ChangePointDetectionExtensible.CPDriver {

    public DealPELTDriver() {
        super();
        jobCount = 3;

        joinMapperClass = CatDepAnioHashJoinMapper.class;
        changePointDetectionReducer = PELTPhase1Reducer.class;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DealPELTDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public void configureJob(Job job, int i) throws Exception {
        job.setJarByClass(DealPELTDriver.class);

        if (i == 1) {
            super.configureJob(job, i); // join cache
        } else if (i == 2) {
            // Para leer lo del job anterior
            job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            // Reagrupamos los segmentos que se habían particionado antes
            job.setMapperClass(PELTPhase2Maper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TextArrayWritable.class);

            // Corremos la fase 2 de PELT Deal
            job.setReducerClass(PELTPhase2Reducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(TextArrayWritable.class);
            job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
        } else if (i == 3) {
            job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            job.setMapperClass(ConclusionStep.M.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TextArrayWritable.class);

            // job.setCombinerClass(job_combine_class);
            job.setReducerClass(ConclusionStep.R.class);
            job.setNumReduceTasks(1); // Solo un reducer, para que solo haya un archivo de salida

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }
    }

    public Text getNewKey(Text key) {
        CustomKey oldKey = new CustomKey(key);

        // Armo clave de nuevo, pero descartando codigoLocal (no representa el local,
        // sino el segmento de DealPelt)
        CustomKey newKey = new CustomKey(
                oldKey.categoria,
                oldKey.departamento,
                oldKey.fecha,
                oldKey.codigoProd,
                CustomKey.NULL_I);

        return newKey.text;
    }
}

class PELTPhase1Reducer extends DistributedPELTReducer {
    // Mismo código que en SimplePELTDriver, sólo cambia la granularidad de la clave
    // y eso es transparente con
    // la implementación de CustomKey

    @Override
    public TextArrayWritable generateValuesOutput(Iterable<Text> values, Text[] changePointsArray) {
        List<Text> dataPoints = new ArrayList<>();
        for (Text value : values) {
            dataPoints.add(value);
        }
        dataPoints.add(new Text("BREAK"));
        for (int i = 0; i < changePointsArray.length; i++) {
            dataPoints.add(changePointsArray[i]);
        }
        Text[] finalArray = new Text[dataPoints.size()];
        dataPoints.toArray(finalArray);

        return new TextArrayWritable(finalArray);
    }
}

class PELTPhase2Maper extends Mapper<Text, Text, Text, TextArrayWritable> {
    @Override
    public void map(Text key, Text detectedChangePoints, Context context)
            throws IOException, InterruptedException {

        CustomKey oldKey = new CustomKey(key);

        // Armo clave de nuevo, pero descartando el segmento de DealPelt
        CustomKey newKey = new CustomKey(
                oldKey.categoria,
                oldKey.departamento,
                oldKey.fecha,
                oldKey.codigoProd,
                CustomKey.NULL_I);

        String[] detectedChangePointsArrayString = detectedChangePoints.toString().split(", ");
        Text[] detectedChangePointsArrayText = new Text[detectedChangePointsArrayString.length];
        for (int i = 0; i < detectedChangePointsArrayString.length; i++) {
            detectedChangePointsArrayText[i] = new Text(detectedChangePointsArrayString[i]);
        }

        // Escribo la nueva clave con los puntos de cambio detectados en la clave
        // anterior (o sea, por producto y por segmento)
        context.write(newKey.text, new TextArrayWritable(detectedChangePointsArrayText));
    }
}

class PELTPhase2Reducer extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {
    @Override
    public void reduce(Text key, Iterable<TextArrayWritable> changePointsSegmento, Context context)
            throws IOException, InterruptedException {
        // La clave es la nueva clave, que no tiene codigoProd ni codigoLocal
        // Los valores son los puntos de cambio detectados en el paso anterior con una
        // granularidad más fina.
        // Los uno todos para extraer conclusiones a nivel de la nueva clave formada.

        // Conteo de ocurrencias de cada punto de cambio
        Set<Date> detectedChangePoints = new HashSet<>();
        List<DataPoint> totalDataPoints = new ArrayList<>();
        int count = 0;
        for (TextArrayWritable changePoints : changePointsSegmento) {
            // changePoints es de la forma:
            // [DataPoint1, DataPoint2, ..., DataPointN, "BREAK", CP1, CP2, ..., CPN]

            count++;
            boolean breakFlag = false;
            for (Text changePoint : changePoints.get()) {
                if (changePoint.toString().equals("BREAK")) {
                    breakFlag = true;
                    continue;
                }
                if (breakFlag) {
                    try {
                        detectedChangePoints.add(DataPoint.formater.parse(changePoint.toString()));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        totalDataPoints.add(new DataPoint(changePoint));
                    } catch (ParseException e) {
                        // Nada
                    }
                }
            }
        }
        List<Date> changePoints = null;
        if (count > 1) {
            // Si hay más de un segmento, ordeno por fecha y ejecuto el algoritmo

            // Ordenamos por fecha
            Collections.sort(totalDataPoints, new Comparator<DataPoint>() {
                @Override
                public int compare(DataPoint x1, DataPoint x2) {
                    return x1.date.compareTo(x2.date);
                }
            });

            // Invocamos la detección de puntos de cambio
            print("Invocando algoritmo de detección de puntos de cambio para la clave '" + key.toString() + "'.");
            RunnableAlgo2 timeredRun = new RunnableAlgo2(totalDataPoints, detectedChangePoints);
            /*
             * 
             * Thread t = new Thread(timeredRun);
             * t.setDaemon(true);
             * t.start();
             * try {
             * t.join(Constants.MAX_SECONDS_CHANGEPOINT_EXECUTION * 1000);
             * } catch (InterruptedException e) {
             * e.printStackTrace();
             * }
             */
            timeredRun.changePoints = CorePELTDeal.detectChangepoints(totalDataPoints, detectedChangePoints);

            print("Fin del algoritmo de detección de puntos de cambio para la clave '" + key.toString() + "'.");

            changePoints = timeredRun.changePoints;
        } else {
            // Si hay un solo segmento, no tiene sentido ejecutar el algoritmo

            changePoints = new ArrayList<>(detectedChangePoints);
        }

        if (changePoints == null) {
            print("Timeout en el algoritmo");
            Text[] changePointsArray = new Text[1];
            changePointsArray[0] = new Text("Timeout");
            context.write(key, new TextArrayWritable(changePointsArray));
        } else if (!changePoints.isEmpty()) {
            print("Puntos de cambio detectados");
            Text[] changePointsArray = new Text[changePoints.size()];
            for (int i = 0; i < changePoints.size(); i++) {
                changePointsArray[i] = new Text(DataPoint.formater.format(changePoints.get(i)).trim());
            }
            context.write(key, new TextArrayWritable(changePointsArray));
        } else {
            print("Ningun punto detectado");
            Text[] changePointsArray = new Text[1];
            changePointsArray[0] = new Text("NoChangePoint");
        }
    }

    static class RunnableAlgo2 implements Runnable {
        List<DataPoint> dataPoints = null;
        Set<Date> validDates = null;
        List<Date> changePoints = null;

        public RunnableAlgo2(List<DataPoint> dataPoints, Set<Date> validDates) {
            this.dataPoints = dataPoints;
            this.validDates = validDates;
        }

        @Override
        public void run() {
            System.out.println("Ejecutando...");
            try {
                changePoints = CorePELTDeal.detectChangepoints(dataPoints, validDates);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void print(String s) {
        System.out.println(s);
    }
}

class JoinMapperPELTSegmentation extends HdfsHashJoinMapper {
    long segmento = 0; // Repetido por Mapper corriendo en paralelo. -> Cada reducer de F1 tiene tantos
                       // precios como mapper en paralelo

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        segmento = 0;
    }

    @Override
    public Text getResultKey() {
        segmento = (segmento + 1) % Constants.QTY_NODES_IN_CLUSTER; // QTY_NODES_IN_CLUSTER segmentos

        return new CustomKey( // Cat, año y producto
                cacheHdfs.baseProductos.get(ventasParser.clave_producto),
                ventasParser.fecha.substring(0, 4),
                ventasParser.clave_producto, segmento).text;
    }
}

class CorePELTDeal {
    // FUENTE: https://aakinshin.net/posts/edpelt/

    // Dado un array de valores `double`, detecta las ubicaciones de los puntos de
    // cambio que dividen la serie original de valores en segmentos
    // "estadísticamente homogéneos".
    // Estos puntos corresponden a momentos en los que las propiedades estadísticas
    // de la distribución están cambiando.
    //
    // Este método admite distribuciones no paramétricas y tiene una complejidad
    // algorítmica de O(N*log(N)).
    //
    // Parámetros:
    // - `data`: un array de valores `double` que representan la serie de tiempo.
    // - `minDistanceBetweenChangepoint`: la distancia mínima entre los puntos de
    // cambio detectados. Debe estar en el rango de 1 a `data.Length`.
    //
    // Devuelve un array `int[]` con índices basados en 0 de los puntos de cambio.
    // Los puntos de cambio corresponden al final de los segmentos detectados.
    // Por ejemplo, los puntos de cambio para {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1,
    // 2, 2, 2, 2, 2, 2} son {5, 11}.
    public static List<Date> detectChangepoints(List<DataPoint> dataPoints, Set<Date> validOptions) throws IOException {
        // Ademas, no consideramos para el calculo los puntos que no representan un
        // cambio en la fecha
        // dataPoints.get(i).date.equals(dataPoints.get(i+1).date)

        double[] data = new double[dataPoints.size()];
        for (int i = 0; i < dataPoints.size(); i++) {
            data[i] = dataPoints.get(i).value;
        }

        // `n` es la longitud de la serie de tiempo
        int n = data.length;
        int minDistanceBetweenChangepoint = Constants.MIN_DAYS_BETWEEN_CHANGE_POINTS;

        // Si la serie de tiempo es demasiado corta, no tiene sentido buscar puntos de
        // cambio
        if (n <= 2)
            return new ArrayList<Date>();
        if (minDistanceBetweenChangepoint < 1 || minDistanceBetweenChangepoint > n)
            throw new IOException("minDistance debería estar en el rango 1 .. data.length");

        // Penalty es un valor constante que se agrega al costo final por cada punto de
        // cambio adicional. Aquí usamos el Criterio de Información Bayesiano
        // Modificado.
        double penalty = 3 * Math.log(n);

        // 'k' es el número de cuantiles que usamos para aproximar una integral durante
        // la evaluación del costo del segmento
        // Usamos `k=Ceiling(4*log(n))` como se sugiere en la Sección 4.3 "Elección de K
        // en ED-PELT" en [Haynes2017]
        // `k` no puede ser mayor que `n`, por lo que siempre debemos usar la función
        // `Min` aquí (importante para n <= 8)
        int k = Math.min(n, (int) Math.ceil(4 * Math.log(n)));

        // Precalculamos las sumas para la función de distribución empírica, lo que nos
        // permitirá evaluar rápidamente el costo del segmento.
        int[][] partialSums = getPartialSums(data, k);

        // Desde aquí, usamos el mismo valor de `partialSums`, `k`, `n` todo el tiempo.
        // No necesitamos cambiarlos.

        // Usamos `tau` para nombrar las variables que son candidatos a puntos de
        // cambio.

        // Usaremos la programación dinámica para encontrar la mejor solución;
        // `bestCost`
        // es el array de costos.

        // `bestCost[i]` es el costo para el subarray `data[0..i-1]`.
        // El indice `i` es 1-based, (`data[0]`..`data[n-1]` corresponden a
        // `bestCost[1]`..`bestCost[n]`)
        double[] bestCost = new double[n + 1];
        bestCost[0] = -penalty;
        for (int currentTau = minDistanceBetweenChangepoint; currentTau < 2
                * minDistanceBetweenChangepoint && currentTau <= n; currentTau++)
            bestCost[currentTau] = getSegmentCost(partialSums, 0, currentTau);

        // `previousChangePointIndex` es un array de referencias a puntos de cambio
        // anteriores. Nos permitirá reconstruir la lista de
        // puntos de cambio al final del algoritmo. Para cada punto de cambio en la
        // posición `i`, el segmento anterior termina en la posición
        // `previousChangePointIndex[i]`. Es un array basado en 1 (`data[0]..`data[n-1]
        // corresponden a `previousChangePointIndex[1]`..`previousChangePointIndex[n]`)
        int[] previousChangePointIndex = new int[n + 1];

        // Usamos PELT (Pruned Exact Linear Time) que significa que en lugar de
        // enumerar todos los valores de tau anteriores posibles,
        // usamos una whitelist de valores de tau "buenos" que se pueden usar en la
        // reconstrucción de la solución óptima.
        // Si estamos 100% seguros de que algunos valores de tau no nos ayudarán a
        // encontrar la solución óptima, los eliminamos de la lista.
        ArrayList<Integer> previousTaus = new ArrayList<Integer>(n + 1);
        previousTaus.add(0);
        previousTaus.add(minDistanceBetweenChangepoint);
        List<Double> costForPreviousTau = new ArrayList<Double>(n + 1);

        // Siguiendo el enfoque de programación dinámica, enumeramos todas las
        // posiciones de tau.
        // Para cada `currentTau`, pretendemos
        // que es el final del último segmento e intentamos encontrar el final del
        // segmento anterior.
        for (int currentTau = 2 * minDistanceBetweenChangepoint; currentTau <= n; currentTau++) {
            // Para cada tau previo, debemos calcular el costo de tomar este tau como el
            // final del anterior segmento.
            // El costo es la suma de tres partes:
            // 1. El costo para el segmento anterior (el segmento que termina en
            // `previousTau`, almacenado en `bestCost[previousTau]`)
            // 2. El costo por el nuevo segmento (de `previousTau` a `currentTau`)
            // 3. La penalización por el nuevo punto de cambio.
            costForPreviousTau.clear();

            for (Integer previousTau : previousTaus) {
                if (!validOptions.contains(dataPoints.get(previousTau).date))
                    // Si previousTau no es una fecha válida, lo ignoramos.
                    costForPreviousTau.add(Double.MAX_VALUE);
                else if (previousTau - 1 >= 0 && previousTau < dataPoints.size()
                        && dataPoints.get(previousTau - 1).date.equals(dataPoints.get(previousTau).date))
                    // Si previousTau no representa un cambio en la fecha, lo
                    // ignoramos.
                    costForPreviousTau.add(Double.MAX_VALUE);
                else
                    costForPreviousTau.add(
                            bestCost[previousTau] + getSegmentCost(partialSums, previousTau, currentTau)
                                    + penalty);
            }

            // Ahora deberíamos elegir el tau que proporcione el costo mínimo posible.
            int bestPreviousTauIndex = whichMin(costForPreviousTau);
            bestCost[currentTau] = costForPreviousTau.get(bestPreviousTauIndex);
            previousChangePointIndex[currentTau] = previousTaus.get(bestPreviousTauIndex);

            // Fase de pruning: eliminamos los valores de tau "inútiles" que no ayudarán a
            // encontrar la solución óptima.
            double currentBestCost = bestCost[currentTau];
            int newPreviousTausSize = 0;
            for (int i = 0; i < previousTaus.size(); i++)
                if (costForPreviousTau.get(i) < currentBestCost + penalty)
                    // Si el i-ésimo tau es "bueno", lo mantenemos en la lista de "buenos" taus.
                    previousTaus.set(newPreviousTausSize++, previousTaus.get(i));

            if (newPreviousTausSize <= previousTaus.size() - newPreviousTausSize)
                previousTaus.subList(newPreviousTausSize, previousTaus.size() - newPreviousTausSize).clear();

            // Añadimos un nuevo valor de tau, que está a una distancia mínima de
            // `minDistance` del siguiente `currentTau`.
            previousTaus.add(currentTau - (minDistanceBetweenChangepoint - 1));
        }

        // Reconstruimos la lista de puntos de cambio.
        ArrayList<Integer> changePointIndexes = new ArrayList<Integer>();
        int currentIndex = previousChangePointIndex[n];
        int whileCounter = 0;
        while (currentIndex != 0) {
            changePointIndexes.add(currentIndex - 1); // // Transformamos el índice basado en 1 a 0
            currentIndex = previousChangePointIndex[currentIndex];
            whileCounter++;
            if (whileCounter > n) {
                throw new IOException("Bucle infinito");
            }
        }
        Collections.reverse(changePointIndexes); // Invertimos la lista para que los índices estén en orden ascendente

        Integer[] result = new Integer[changePointIndexes.size()];
        changePointIndexes.toArray(result);

        List<Date> changePoints = new ArrayList<Date>();
        for (int i = 0; i < result.length; i++) {
            changePoints.add(dataPoints.get(result[i]).date);
        }
        return changePoints;
    }

    private static int[][] getPartialSums(double[] data, int k) {
        int n = data.length;
        int[][] partialSums = new int[k][n + 1];

        double[] sortedData = new double[n];
        List<Double> sortedDataList = new ArrayList<Double>();
        for (int i = 0; i < n; i++)
            sortedDataList.add(data[i]);
        Collections.sort(sortedDataList);
        for (int i = 0; i < n; i++)
            sortedData[i] = sortedDataList.get(i);

        for (int i = 0; i < k; i++) {
            double z = -1 + (2 * i + 1.0) / k; // Valores desde (-1+1/k) a (1-1/k) con paso = 2/k
            double p = 1.0 / (1 + Math.pow(2 * n - 1, -z)); // Valores entre 0.0 y 1.0
            double t = sortedData[(int) ((n - 1) * p)]; // Valor de cuantil

            for (int tau = 1; tau <= n; tau++) {
                partialSums[i][tau] = partialSums[i][tau - 1];
                if (data[tau - 1] < t)
                    partialSums[i][tau] += 2;
                if (data[tau - 1] == t)
                    partialSums[i][tau] += 1;
            }
        }
        return partialSums;
    }

    // Calcula el costo del segmento (tau1; tau2].
    private static double getSegmentCost(int[][] partialSums, int tau1, int tau2) {
        int k = partialSums.length;
        int n = partialSums[0].length - 1;

        double sum = 0;
        for (int i = 0; i < k; i++) {
            int actualSum = partialSums[i][tau2] - partialSums[i][tau1];

            if (actualSum != 0 && actualSum != (tau2 - tau1) * 2) {
                double fit = actualSum * 0.5 / (tau2 - tau1);
                double lnp = (tau2 - tau1) * (fit * Math.log(fit) + (1 - fit) * Math.log(1 - fit));
                sum += lnp;
            }
        }
        double c = -Math.log(2 * n - 1);
        return 2.0 * c / k * sum;
    }

    // Retorna el índice del mínimo elemento en la lista de valores.
    // En caso de que haya varios elementos mínimos en la lista dada, se devolverá
    // el índice del primero.
    private static int whichMin(List<Double> values) throws IOException {
        if (values.size() == 0)
            throw new IOException("Array no debería estar vacío");

        double minValue = values.get(0);
        int minIndex = 0;
        for (int i = 1; i < values.size(); i++)
            if (values.get(i) < minValue) {
                minValue = values.get(i);
                minIndex = i;
            }

        return minIndex;
    }
}