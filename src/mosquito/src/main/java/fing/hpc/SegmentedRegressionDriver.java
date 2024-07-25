package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.util.ToolRunner;

class SegmentedRegressionReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Convertimos en DataPoints
		List<DataPoint> dataPoints = new ArrayList<>();
		for (Text k_v : values) {
			ParText vars = new ParText(k_v);
			try {
				dataPoints.add(new DataPoint(vars.x, Float.parseFloat(vars.y)));
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
		List<String> changePoints = MathPart.detectChangepoints(dataPoints);

		context.write(key, new Text(changePoints.toString()));
	}
}

public class SegmentedRegressionDriver extends CacheHdfs.CDriver {

	@Override
	public void configureJob(Job job, int i) throws Exception {
		super.configureJob(job, i);
		if (i == 1) {
			job.setMapperClass(HdfsHashJoinMapper.class);
			// job.setCombinerClass(job_combine_class);
			job.setReducerClass(GroupBy.GroupByAvgReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		} else if (i == 2) {
			job.setJarByClass(SegmentedRegressionDriver.class);

			job.setMapperClass(IdentityMapper.class);
			// job.setCombinerClass(job_combine_class);
			job.setReducerClass(SegmentedRegressionReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SegmentedRegressionDriver(), args);
		System.exit(exitCode);
	}
}

class MathPart {
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
				changepoints.add(DataPoint.formater.format(resDate));
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
