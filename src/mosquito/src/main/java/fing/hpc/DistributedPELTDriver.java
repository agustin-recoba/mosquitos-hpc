package fing.hpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;

class DistributedPELTReducer extends ChangePointDetectionReducer {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		changePointDetectionAlgorithm = new CorePELT();
	}
}

public class DistributedPELTDriver extends ChangePointDetectionDriver {
	public DistributedPELTDriver() {
		super();
		changePointDetectionReducer = DistributedPELTReducer.class;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DistributedPELTDriver(), args);
		System.exit(exitCode);
	}
}

class CorePELT implements ChangePointDetectionAlgorithm {
	// FUENTE: https://aakinshin.net/posts/edpelt/

	@Override
	public List<Date> detectChangePoints(List<DataPoint> dataPoints) {
		try {
			return detectChangepoints(dataPoints);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null; // ToDo
		}
	}

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
	public static List<Date> detectChangepoints(List<DataPoint> dataPoints) throws Exception {
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
			throw new Exception("minDistance debería estar en el rango 1 .. data.length");

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
				* minDistanceBetweenChangepoint; currentTau++)
			bestCost[currentTau] = getSegmentCost(partialSums, 0, currentTau, k, n);

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
		for (int currentTau = 2 * minDistanceBetweenChangepoint; currentTau < n + 1; currentTau++) {
			// Para cada tau previo, debemos calcular el costo de tomar este tau como el
			// final del anterior segmento.
			// El costo es la suma de tres partes:
			// 1. El costo para el segmento anterior (el segmento que termina en
			// `previousTau`, almacenado en `bestCost[previousTau]`)
			// 2. El costo por el nuevo segmento (de `previousTau` a `currentTau`)
			// 3. La penalización por el nuevo punto de cambio.
			costForPreviousTau.clear();

			for (Integer previousTau : previousTaus) {
				if (dataPoints.get(currentTau).date.equals(dataPoints.get(currentTau + 1).date)) {
					// Si la fecha es la misma, no se considera un cambio
					costForPreviousTau.add(Double.MAX_VALUE);
				} else {
					costForPreviousTau.add(
							bestCost[previousTau] + getSegmentCost(partialSums, previousTau, currentTau, k, n)
									+ penalty);
				}
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
		while (currentIndex != 0) {
			changePointIndexes.add(currentIndex - 1); // // Transformamos el índice basado en 1 a 0
			currentIndex = previousChangePointIndex[currentIndex];
		}
		Collections.reverse(changePointIndexes); // Invertimos la lista para que los índices estén en orden ascendente

		Integer[] result = new Integer[changePointIndexes.size()];
		changePointIndexes.toArray(result);

		List<Date> changePoints = new ArrayList<Date>();
		try {
			for (int i = 0; i < result.length; i++) {
				changePoints.add(dataPoints.get(result[i]).date);
			}
		} catch (Exception e) {
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
	private static double getSegmentCost(int[][] partialSums, int tau1, int tau2, int k, int n) {
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
	private static int whichMin(List<Double> values) throws Exception {
		if (values.size() == 0)
			throw new Exception("Array no debería estar vacío");

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