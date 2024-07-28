package fing.hpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.util.ToolRunner;

class SegmentedRegressionReducer extends ChangePointDetectionReducer {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		changePointDetectionAlgorithm = new CoreSegementedRegression();
	}
}

public class SegmentedRegressionDriver extends ChangePointDetectionDriver {
	public SegmentedRegressionDriver() {
		super();
		changePointDetectionReducer = SegmentedRegressionReducer.class;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SegmentedRegressionDriver(), args);
		System.exit(exitCode);
	}
}

class CoreSegementedRegression implements ChangePointDetectionAlgorithm {

	@Override
	public List<Date> detectChangePoints(List<DataPoint> orderedData) throws IOException, InterruptedException {
		List<Date> changepoints = new ArrayList<>();
		int n = orderedData.size();

		for (int i = 1; i < n - 1; i++) {
			List<DataPoint> leftSegment = orderedData.subList(0, i + 1);
			List<DataPoint> rightSegment = orderedData.subList(i + 1, n);
			// Chequeo que el ultimo punto de la izquierda no tenga el mismo timestamp que
			// el primero de la derecha
			if (leftSegment.get(leftSegment.size() - 1).date.equals(rightSegment.get(0).date))
				continue;

			double leftError = calculateSegmentError(leftSegment);
			double rightError = calculateSegmentError(rightSegment);

			double totalError = leftError + rightError;
			double currentError = calculateSegmentError(orderedData);

			if (judgePoint(totalError, currentError)) {
				Date resDate = orderedData.get(i).date;
				changepoints.add(resDate);
			}
		}

		return changepoints;
	}

	private static boolean judgePoint(double segmentationError, double noSegmentationError) {
		return segmentationError < 0.8 * noSegmentationError;
	}

	private static double calculateSegmentError(List<DataPoint> segment) {
		int n = segment.size();
		if (n < Constants.MIN_DAYS_BETWEEN_CHANGE_POINTS)
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
