package fing.hpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupBy {
	public static class GroupByAvgReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Calculamos precio medio por día
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
			
			for (String date : sums.keySet()) {
				context.write(key, (new ParText(date, Float.toString((sums.get(date) / counts.get(date)))).text));
			}	
		}
	}
	
	public static class GroupBySumReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Calculamos precio medio por día
			Map<String, Float> sums = new HashMap<>();
			for (Text t : values) {
				ParText vars = new ParText(t);
				Float sum = sums.get(vars.x);
				if (sum == null) {
					sums.put(vars.x, Float.parseFloat(vars.y));
				} else {
					sums.put(vars.x, sum + Float.parseFloat(vars.y));
				}
			}
			
			for (String date : sums.keySet()) {
				context.write(key, (new ParText(date, Float.toString((sums.get(date)))).text));
			}
		}
	}
}