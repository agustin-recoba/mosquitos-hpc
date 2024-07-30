package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupByReducer {
	public static class Average extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Valor medio por sub-clave
			Map<String, Float> sums = new HashMap<>();
			Map<String, Integer> counts = new HashMap<>();
			for (Text t : values) {
				ValuePair vars = new ValuePair(t);
				Integer count = counts.get(vars.x);
				if (count == null) {
					counts.put(vars.x, 1);
					sums.put(vars.x, Float.parseFloat(vars.y));
				} else {
					counts.put(vars.x, count + 1);
					sums.put(vars.x, sums.get(vars.x) + Float.parseFloat(vars.y));
				}
			}

			for (String date : sums.keySet()) {
				context.write(key, (new ValuePair(date, Float.toString((sums.get(date) / counts.get(date)))).text));
			}
		}
	}

	public static class ValueCount extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Contamos
			Map<String, Integer> counts = new HashMap<>();
			for (Text t : values) {
				ValuePair vars = new ValuePair(t);
				Integer count = counts.get(vars.x);
				if (count == null) {
					counts.put(vars.x, 1);
				} else {
					counts.put(vars.x, count + 1);
				}
			}

			for (String date : counts.keySet()) {
				context.write(key, (new ValuePair(date, Float.toString(counts.get(date))).text));
			}
		}
	}

	public static class SumAllBySubKey extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Calculamos suma total por sub-clave
			Map<String, Float> sums = new HashMap<>();
			for (Text t : values) {
				ValuePair vars = new ValuePair(t);
				Float sum = sums.get(vars.x);
				if (sum == null) {
					sums.put(vars.x, Float.parseFloat(vars.y));
				} else {
					sums.put(vars.x, sum + Float.parseFloat(vars.y));
				}
			}

			for (String date : sums.keySet()) {
				context.write(key, (new ValuePair(date, Float.toString((sums.get(date)))).text));
			}
		}
	}

	public static class SumAll extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Calculamos suma total por sub-clave
			Float sum = 0.0f;
			for (Text t : values) {
				ValuePair vars = new ValuePair(t);
				sum += Float.parseFloat(vars.y);
			}

			context.write(key, new Text(sum.toString()));
		}
	}

	public static class SumAll2 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Calculamos suma total por sub-clave
			Float sum = 0.0f;
			for (Text t : values) {
				try {
					sum += Float.parseFloat(t.toString());
				} catch (NumberFormatException e) {
				}
			}

			context.write(key, new Text(sum.toString()));
		}
	}

	public static class MaxFloatReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {

			float maxPrice = Float.MIN_VALUE;
			for (FloatWritable valor : values) {
				maxPrice = Math.max(maxPrice, valor.get());
			}
			context.write(key, new FloatWritable(maxPrice));
		}
	}

}