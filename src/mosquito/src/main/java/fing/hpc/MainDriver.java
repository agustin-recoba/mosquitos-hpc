package fing.hpc;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class ProductosParser {
    public String categoria;
    public int clave;

    public void parse(String record) {
    	var records = record.split(";", 0);
    	
        categoria = records[0];
        clave = Integer.parseInt(records[1]);
    }

    public void parse(Text record) {
        parse(record.toString());
    }
}

class LocalesParser {

    public String departamento;
    public int clave;

    public void parse(String record) {
    	var records = record.split(";", 0);
    	
    	departamento = records[0];
        clave = Integer.parseInt(records[1]);
    }

    public void parse(Text record) {
        parse(record.toString());
    }
}


class VentasParser {
    public int clave_local;
    public int clave_producto;
    public LocalDate fecha;
    public int cant_vta_original;
    public int cant_vta;
    public float precio_unitario;
    public int clave_venta;

    public void parse(String record) {
    	var records = record.split(";", 0);
    	
    	clave_local = Integer.parseInt(records[0]);
    	clave_producto = Integer.parseInt(records[1]);
    	fecha = LocalDate.parse(records[2]);
    	cant_vta_original = Integer.parseInt(records[3]);
    	cant_vta = Integer.parseInt(records[4]);
    	precio_unitario = Float.parseFloat(records[5]);
    	clave_venta = Integer.parseInt(records[6]);
    }

    public void parse(Text record) {
        parse(record.toString());
    }
}


class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
            Context context)
            throws IOException, InterruptedException {

        int maxValue = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(key, new IntWritable(maxValue));
    }
}

class ReadHDFSJoinMapper
        extends Mapper<LongWritable, Text, Text, FloatWritable> {

    enum Temperature {
        MALFORMED
    }

    private LocalesParser localesParser = new LocalesParser();
    private ProductosParser productosParser = new ProductosParser();
    private VentasParser ventasParser = new VentasParser();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	// HACER JOIN
    	
    	ventasParser.parse(value);
        context.write(new Text(Integer.toString(ventasParser.clave_producto)), new FloatWritable(ventasParser.precio_unitario));
    }
}

public class MainDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Max temperature");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ReadHDFSJoinMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MainDriver(), args);
        System.exit(exitCode);
    }
}