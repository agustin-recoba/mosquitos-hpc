package fing.hpc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class ReduceJoinDriver2 extends Configured implements Tool {
    int jobCount = -1;

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Uso: %s <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        System.out.println("Cantidad de jobs a ejecutar: " + jobCount);

        Job job = Job.getInstance(getConf(), "HPC - Mosquitos - " + 1);
        job.setJarByClass(getClass());
        job.setNumReduceTasks(2 * Constants.QTY_NODES_IN_CLUSTER);

        MultipleInputs.addInputPath(job, new Path("/data/productos.csv"), TextInputFormat.class,
                ReduceJoinMapper.ProductosMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
                ReduceJoinMapper.ProductoVentasMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ProductoReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path("/tmp/" + args[1] + 1));

        job.waitForCompletion(true);

        // FASE 2:
        Job job2 = Job.getInstance(getConf(), "HPC - Mosquitos - " + 2);
        job2.setJarByClass(getClass());
        job2.setNumReduceTasks(2 * Constants.QTY_NODES_IN_CLUSTER);

        // Reseteo la clase reducer

        MultipleInputs.addInputPath(job2, new Path("/tmp/" + args[1] + 1), TextInputFormat.class,
                ReduceJoinMapper.LocalVentaMapper.class);

        MultipleInputs.addInputPath(job2, new Path("/data/locales.csv"), TextInputFormat.class,
                ReduceJoinMapper.LocalesMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(LocalReduceJoinReducer.class);
        FileOutputFormat.setOutputPath(job2, new Path("/tmp/" + args[1] + 2));

        job2.waitForCompletion(true);

        // FASE 3:
        // JOB SUMARIZACION FINAL

        Job job3 = Job.getInstance(getConf(), "HPC - Mosquitos - " + 3);
        job3.setJarByClass(getClass());
        job3.setNumReduceTasks(2 * Constants.QTY_NODES_IN_CLUSTER);

        FileInputFormat.addInputPath(job3, new Path("/tmp/" + args[1] + 2));
        job3.setInputFormatClass(TextInputFormat.class);

        job3.setMapperClass(IdentityMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(GroupByReducer.SumAll2.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        return job3.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ReduceJoinDriver2(), args);
        System.exit(exitCode);
    }
}
