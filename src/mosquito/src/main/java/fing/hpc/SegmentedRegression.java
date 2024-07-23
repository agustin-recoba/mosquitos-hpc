package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class SegmentedRegression {

	public static class SegmentedRegressionReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Map<String, Float> sums = new HashMap<>();
			Map<String, Integer> counts = new HashMap<>();
			for (Text t : values) {
				ParText vars = new ParText(t);
				Integer count = counts.get(vars.x);
				if (count == null) {
					counts.put(vars.x, 1);
					sums.put(vars.x, Float.parseFloat(vars.y));
				} else {
					counts.put(vars.x, count + 1);
					sums.put(vars.x, sums.get(vars.x) + Float.parseFloat(vars.y));
				}
			}
			List<DataPoint> dataPoints = new ArrayList<>();
			for (String date : sums.keySet()) {
				try {
					dataPoints.add(new DataPoint(date, sums.get(date) / counts.get(date)));
				} catch (ParseException e) {
					// Seguir
				}
			}
			// Sorting
			Collections.sort(dataPoints, new Comparator<DataPoint>() {
				@Override
				public int compare(DataPoint x1, DataPoint x2) {

					return x1.date.compareTo(x2.date);
				}
			});

			List<String> changePoints = detectChangepoints(dataPoints);

			context.write(key, new Text(changePoints.toString()));
		}
	}

	static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public static class DataPoint {
		Date date;
		float value;

		public DataPoint(String dateStr, float value) throws ParseException {

			this.date = sdf.parse(dateStr);
			this.value = value;
		}
	}

	public static List<String> detectChangepoints(List<DataPoint> orderedData) {
		List<String> changepoints = new ArrayList<>();
		int n = orderedData.size();

		for (int i = 1; i < n - 1; i++) {
			List<DataPoint> leftSegment = orderedData.subList(0, i + 1);
			List<DataPoint> rightSegment = orderedData.subList(i + 1, n);

			double leftError = calculateSegmentError(leftSegment);
			double rightError = calculateSegmentError(rightSegment);

			double totalError = leftError + rightError;
			double currentError = calculateSegmentError(orderedData);

			if (judgePoint(totalError, currentError)) {
				Date resDate = orderedData.get(i).date;
				changepoints.add(sdf.format(resDate));
			}
		}

		return changepoints;
	}

	private static boolean judgePoint(double segmentationError, double noSegmentationError) {
		return segmentationError < 0.8 * noSegmentationError;
	}

	private static double calculateSegmentError(List<DataPoint> segment) {
		int n = segment.size();
		if (n < 2)
			return Float.POSITIVE_INFINITY;

		double sumX = 0;
		double sumY = 0;
		double sumXY = 0;
		double sumXX = 0;

		for (DataPoint dp : segment) {
			long x = dp.date.getTime();
			double y = dp.value;

			sumX += x;
			sumY += y;
			sumXY += x * y;
			sumXX += x * x;
		}

		double slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
		double intercept = (sumY - slope * sumX) / n;

		double error = 0;
		for (DataPoint dp : segment) {
			long x = dp.date.getTime();
			double y = dp.value;
			double predictedY = slope * x + intercept;
			error += Math.pow(y - predictedY, 2);
		}

		return error;
	}
}