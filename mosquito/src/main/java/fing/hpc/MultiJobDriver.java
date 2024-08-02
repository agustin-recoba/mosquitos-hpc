package fing.hpc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
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
		System.out.println("Cantidad de jobs a ejecutar: " + jobCount);

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

			job.setNumReduceTasks(2 * Constants.QTY_NODES_IN_CLUSTER);

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