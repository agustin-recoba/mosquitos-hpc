package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class MultiJobDriver extends Configured implements Tool {
	int jobCount = -1;

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Uso: %s <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		System.out.println("Job count for this task: " + jobCount);

		Path prevTempPath = new Path(args[0]);

		int lastCode = 0;
		for (int i = 1; i <= jobCount; i++) {
			Job job = Job.getInstance(getConf(), "HPC - Mosquitos - " + i);
			job.setJarByClass(getClass());

			FileInputFormat.addInputPath(job, prevTempPath);
			System.out.println("Input " + i + ": " + prevTempPath.toString());

			Path output;
			if (i == jobCount) {// Si es el ultimo job
				output = new Path(args[1]);
			} else {
				output = new Path("/tmp/" + args[1] + i);
			}
			FileOutputFormat.setOutputPath(job, output);
			System.out.println("Output " + i + ": " + output.toString());

			configureJob(job, i);

			lastCode = job.waitForCompletion(true) ? 0 : 1;
			prevTempPath = output;
		}
		return lastCode;
	}

	public void configureJob(Job job, int i) throws Exception {
		// USAR PARA SETEAR MAPPERS, REDUCERS, ETC
	}
}

class TextArrayWritable extends ArrayWritable {

	public TextArrayWritable() {
		super(Text.class);
	}

	public TextArrayWritable(Text[] values) {
		super(Text.class, values);
	}

	@Override
	public Text[] get() {
		Writable[] writables = super.get();
		Text[] values = new Text[writables.length];
		for (int i = 0; i < writables.length; i++) {
			values[i] = (Text) writables[i];
		}

		return values;
	}

	@Override
	public String toString() {
		Text[] values = get();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < values.length; i++) {
			sb.append(values[i].toString());
			if (i < values.length - 1) {
				sb.append(", ");
			}
		}
		return sb.toString();
	}

}

interface ChangePointDetectionAlgorithm {
	abstract List<Date> detectChangePoints(List<DataPoint> dataPoints);
}

class ChangePointDetectionReducer extends Reducer<Text, Text, Text, TextArrayWritable> {
	public ChangePointDetectionAlgorithm changePointDetectionAlgorithm = null;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Convertimos en DataPoints
		List<DataPoint> dataPoints = new ArrayList<>();
		for (Text k_v : values) {
			ValuePair vars = new ValuePair(k_v);
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
		List<Date> changePoints = changePointDetectionAlgorithm.detectChangePoints(dataPoints);
		Text[] changePointsArray = new Text[changePoints.size()];
		for (int i = 0; i < changePoints.size(); i++) {
			changePointsArray[i] = new Text(DataPoint.formater.format(changePoints.get(i)));
		}

		context.write(key, new TextArrayWritable(changePointsArray));
	}
}

class ChangePointDetectionDriver extends CacheHdfs.CDriver {
	public Class<? extends Reducer<Text, Text, Text, TextArrayWritable>> changePointDetectionReducer = null;

	public ChangePointDetectionDriver() {
		super();
		jobCount = 2;
	}

	@Override
	public void configureJob(Job job, int i) throws Exception {
		job.setJarByClass(DistributedPELTDriver.class);

		if (i == 1) {
			super.configureJob(job, i);

			job.setMapperClass(HdfsHashJoinMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			// job.setCombinerClass(job_combine_class);
			job.setReducerClass(changePointDetectionReducer);

			job.setOutputKeyClass(Text.class); // Escribe clave
			job.setOutputValueClass(TextArrayWritable.class); // Escribe fecha y Average(valor)
			job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
		} else if (i == 2) {
			job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ";");

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			job.setMapperClass(ConclusionStep.M.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TextArrayWritable.class);

			// job.setCombinerClass(job_combine_class);
			job.setReducerClass(ConclusionStep.R.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		}

	}
}