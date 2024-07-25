package fing.hpc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class ExtensibleDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Uso: %s <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Path prevTempPath = new Path("/tmp/"+args[1]);
		
		int lastCode = 0;
		for (int i = 1; i <= getJobCount(); i++) {
			Job job = Job.getInstance(getConf(), "HPC - Mosquitos - " + i);
			job.setJarByClass(getClass());
			Path newTempPath = new Path("/tmp/"+args[1]+i);
			
			if (i == 1) // Si es el primer job
				FileInputFormat.addInputPath(job, new Path(args[0]));
			else
				FileInputFormat.addInputPath(job, prevTempPath);
			
			if (i == getJobCount()) // Si es el ultimo job
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
			else
				FileOutputFormat.setOutputPath(job, newTempPath);

			configureJob(job, i);

			lastCode = job.waitForCompletion(true) ? 0 : 1;
			prevTempPath = newTempPath;
		}
		return lastCode;
	}

	static int getJobCount() {
		return 1;
	}

	public void configureJob(Job job, int i) throws Exception {
		// USAR PARA SETEAR MAPPERS, REDUCERS, ETC
	}
}