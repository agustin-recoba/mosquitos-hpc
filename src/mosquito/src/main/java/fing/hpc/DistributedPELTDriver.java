package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

class DistributedPELTReducer extends Reducer<Text, Text, Text, TextArrayWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Convertimos en DataPoints
		List<DataPoint> dataPoints = new ArrayList<>();
		for (Text k_v : values) {
			ValuePair vars = new ValuePair(k_v);
			try {
				dataPoints.add(new DataPoint(vars.x, Double.parseDouble(vars.y)));
			} catch (ParseException e) {
				// Nada
			}
		}

		// Ordenamos por fecha
		Collections.sort(dataPoints, new Comparator<DataPoint>() {
			@Override
			public int compare(DataPoint x1, DataPoint x2) {
				return x1.date.compareTo(x2.date);
			}
		});

		// Invocamos la detección de puntos de cambio
		double[] dataPointsArray = new double[dataPoints.size()];
		for (int i = 0; i < dataPoints.size(); i++) {
			dataPointsArray[i] = dataPoints.get(i).value;
		}

		Integer[] changePoints = CorePELT.detectChangepoints(dataPointsArray, 4);

		Text[] changePointsArray = new Text[changePoints.length];
		for (int i = 0; i < changePoints.length; i++) {
			changePointsArray[i] = new Text(dataPoints.get(changePoints[i]).date.toString());
		}

		context.write(key, new TextArrayWritable(changePointsArray));
	}
}

public class DistributedPELTDriver extends CacheHdfs.CDriver {
	static int jobCount = 2;

	@Override
	public void configureJob(Job job, int i) throws Exception {
		super.configureJob(job, i);
		job.setJarByClass(SegmentedRegressionDriver.class);

		if (i == 1) {
			job.setMapperClass(HdfsHashJoinMapper.class);
			// job.setCombinerClass(job_combine_class);
			job.setReducerClass(GroupByReducer.Average.class);

			job.setOutputKeyClass(Text.class); // Escribe clave
			job.setOutputValueClass(Text.class); // Escribe fecha y Average(valor)
			job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
		} else if (i == 2) {
			job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setMapperClass(IdentityMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			// job.setCombinerClass(job_combine_class);
			job.setReducerClass(SegmentedRegressionReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(TextArrayWritable.class);
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SegmentedRegressionDriver(), args);
		System.exit(exitCode);
	}
}

class CorePELT {
	// FUENTE: https://aakinshin.net/posts/edpelt/

	/// For given array of `double` values, detects locations of changepoints that
	/// splits original series of values into "statistically homogeneous" segments.
	/// Such points correspond to moments when statistical properties of the
	/// distribution are changing.
	///
	/// This method supports nonparametric distributions and has O(N*log(N))
	/// algorithmic complexity.
	///
	/// Returns an `int[]` array with 0-based indexes of changepoint.
	/// Changepoints correspond to the end of the detected segments.
	/// For example, changepoints for { 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2,
	/// 2, 2, 2 } are { 5, 11 }.
	public static Integer[] detectChangepoints(double[] data, int minDistanceBetweenChangepoint) throws IOException {
		// We will use `n` as the number of elements in the `data` array
		int n = data.length;

		// Checking corner cases
		if (n <= 2)
			return new Integer[0];
		if (minDistanceBetweenChangepoint < 1 || minDistanceBetweenChangepoint > n)
			throw new IOException(
					"minDistance should be in range from 1 to data.Length");

		// The penalty which we add to the final cost for each additional changepoint
		// Here we use the Modified Bayesian Information Criterion
		double penalty = 3 * Math.log(n);

		// `k` is the number of quantiles that we use to approximate an integral during
		// the segment cost evaluation
		// We use `k=Ceiling(4*log(n))` as suggested in the Section 4.3 "Choice of K in
		// ED-PELT" in [Haynes2017]
		// `k` can't be greater than `n`, so we should always use the `Min` function
		// here (important for n <= 8)
		int k = Math.min(n, (int) Math.ceil(4 * Math.log(n)));

		// We should precalculate sums for empirical CDF, it will allow fast evaluating
		// of the segment cost
		int[][] partialSums = GetPartialSums(data, k);

		// Since we use the same values of `partialSums`, `k`, `n` all the time,
		// we introduce a shortcut `Cost(tau1, tau2)` for segment cost evaluation.
		// Hereinafter, we use `tau` to name variables that are changepoint candidates.

		// double Cost(int tau1, int tau2) => GetSegmentCost(partialSums, tau1, tau2, k,
		// n);

		// We will use dynamic programming to find the best solution; `bestCost` is the
		// cost array.
		// `bestCost[i]` is the cost for subarray `data[0..i-1]`.
		// It's a 1-based array (`data[0]`..`data[n-1]` correspond to
		// `bestCost[1]`..`bestCost[n]`)
		double[] bestCost = new double[n + 1];
		bestCost[0] = -penalty;
		for (int currentTau = minDistanceBetweenChangepoint; currentTau < 2
				* minDistanceBetweenChangepoint; currentTau++)
			bestCost[currentTau] = GetSegmentCost(partialSums, 0, currentTau, k, n);

		// `previousChangePointIndex` is an array of references to previous
		// changepoints. If the current segment ends at
		// the position `i`, the previous segment ends at the position
		// `previousChangePointIndex[i]`. It's a 1-based
		// array (`data[0]`..`data[n-1]` correspond to the
		// `previousChangePointIndex[1]`..`previousChangePointIndex[n]`)
		int[] previousChangePointIndex = new int[n + 1];

		// We use PELT (Pruned Exact Linear Time) approach which means that instead of
		// enumerating all possible previous
		// tau values, we use a whitelist of "good" tau values that can be used in the
		// optimal solution. If we are 100%
		// sure that some of the tau values will not help us to form the optimal
		// solution, such values should be
		// removed. See [Killick2012] for details.
		ArrayList<Integer> previousTaus = new ArrayList<Integer>(n + 1);
		previousTaus.add(0);
		previousTaus.add(minDistanceBetweenChangepoint);
		List<Double> costForPreviousTau = new ArrayList<Double>(n + 1);

		// Following the dynamic programming approach, we enumerate all tau positions.
		// For each `currentTau`, we pretend
		// that it's the end of the last segment and trying to find the end of the
		// previous segment.
		for (int currentTau = 2 * minDistanceBetweenChangepoint; currentTau < n + 1; currentTau++) {
			// For each previous tau, we should calculate the cost of taking this tau as the
			// end of the previous
			// segment. This cost equals the cost for the `previousTau` plus cost of the new
			// segment (from `previousTau`
			// to `currentTau`) plus penalty for the new changepoint.
			costForPreviousTau.clear();

			for (Integer previousTau : previousTaus)
				costForPreviousTau.add(
						bestCost[previousTau] + GetSegmentCost(partialSums, previousTau, currentTau, k, n) + penalty);

			// Now we should choose the tau that provides the minimum possible cost.
			int bestPreviousTauIndex = WhichMin(costForPreviousTau);
			bestCost[currentTau] = costForPreviousTau.get(bestPreviousTauIndex);
			previousChangePointIndex[currentTau] = previousTaus.get(bestPreviousTauIndex);

			// Prune phase: we remove "useless" tau values that will not help to achieve
			// minimum cost in the future
			double currentBestCost = bestCost[currentTau];
			int newPreviousTausSize = 0;
			for (int i = 0; i < previousTaus.size(); i++)
				if (costForPreviousTau.get(i) < currentBestCost + penalty)
					previousTaus.set(newPreviousTausSize++, previousTaus.get(i));
			previousTaus.subList(newPreviousTausSize, previousTaus.size() - newPreviousTausSize).clear();

			// We add a new tau value that is located on the `minDistance` distance from the
			// next `currentTau` value
			previousTaus.add(currentTau - (minDistanceBetweenChangepoint - 1));
		}

		// Here we collect the result list of changepoint indexes `changePointIndexes`
		// using `previousChangePointIndex`
		ArrayList<Integer> changePointIndexes = new ArrayList<Integer>();
		int currentIndex = previousChangePointIndex[n]; // The index of the end of the last segment is `n`
		while (currentIndex != 0) {
			changePointIndexes.add(currentIndex - 1); // 1-based indexes should be be transformed to 0-based indexes
			currentIndex = previousChangePointIndex[currentIndex];
		}
		Collections.reverse(changePointIndexes); // The result changepoints should be sorted in ascending order.
		return (Integer[]) changePointIndexes.toArray();
	}

	private static int[][] GetPartialSums(double[] data, int k) {
		int n = data.length;
		int[][] partialSums = new int[k][n + 1];

		double[] sortedData = new double[n];

		for (int i = 0; i < k; i++) {
			double z = -1 + (2 * i + 1.0) / k; // Values from (-1+1/k) to (1-1/k) with step = 2/k
			double p = 1.0 / (1 + Math.pow(2 * n - 1, -z)); // Values from 0.0 to 1.0
			double t = sortedData[(int) ((n - 1) * p)]; // Quantile value, formula (2.1) in [Haynes2017]

			for (int tau = 1; tau <= n; tau++) {
				partialSums[i][tau] = partialSums[i][tau - 1];
				if (data[tau - 1] < t)
					partialSums[i][tau] += 2; // We use doubled value (2) instead of original 1.0
				if (data[tau - 1] == t)
					partialSums[i][tau] += 1; // We use doubled value (1) instead of original 0.5
			}
		}
		return partialSums;
	}

	///
	/// Calculates the cost of the (tau1; tau2] segment.
	///
	private static double GetSegmentCost(int[][] partialSums, int tau1, int tau2, int k, int n) {
		double sum = 0;
		for (int i = 0; i < k; i++) {
			// actualSum is (count(data[j] < t) * 2 + count(data[j] == t) * 1) for
			// j=tau1..tau2-1
			int actualSum = partialSums[i][tau2] - partialSums[i][tau1];

			// We skip these two cases (correspond to fit = 0 or fit = 1) because of invalid
			// Math.Log values
			if (actualSum != 0 && actualSum != (tau2 - tau1) * 2) {
				// Empirical CDF $\hat{F}_i(t)$ (Section 2.1 "Model" in [Haynes2017])
				double fit = actualSum * 0.5 / (tau2 - tau1);
				// Segment cost $\mathcal{L}_{np}$ (Section 2.2 "Nonparametric maximum
				// likelihood" in [Haynes2017])
				double lnp = (tau2 - tau1) * (fit * Math.log(fit) + (1 - fit) * Math.log(1 - fit));
				sum += lnp;
			}
		}
		double c = -Math.log(2 * n - 1); // Constant from Lemma 3.1 in [Haynes2017]
		return 2.0 * c / k * sum; // See Section 3.1 "Discrete approximation" in [Haynes2017]
	}

	///
	/// Returns the index of the minimum element.
	/// In case if there are several minimum elements in the given list, the index
	/// of the first one will be returned.
	///
	private static int WhichMin(List<Double> values) throws IOException {
		if (values.size() == 0)
			throw new IOException("Array should contain elements");

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
