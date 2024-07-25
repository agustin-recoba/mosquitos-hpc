package fing.hpc;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.collections.OrderedMap;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

class ProductosParser {
	public String categoria;
	public long clave;

	public void parse(String record) {
		String[] records = record.split(";", 0);

		categoria = records[0];
		clave = Long.parseLong(records[1]);
	}

	public void parse(Text record) {
		parse(record.toString());
	}
}

class LocalesParser {

	public String departamento;
	public long clave;

	public void parse(String record) {
		String[] records = record.split(";", 0);

		departamento = records[0];
		clave = Long.parseLong(records[1]);
	}

	public void parse(Text record) {
		parse(record.toString());
	}
}

class VentasParser {
	public long clave_local;
	public long clave_producto;
	public String fecha;
	public float cant_vta_original;
	public float cant_vta;
	public float precio_unitario;
	public long clave_venta;

	public void parse(String record) {
		String[] records = record.split(";", 0);

		clave_local = Long.parseLong(records[0]);
		clave_producto = Long.parseLong(records[1]);
		fecha = records[2];
		cant_vta_original = Float.parseFloat(records[3]);
		cant_vta = Float.parseFloat(records[4]);
		precio_unitario = Float.parseFloat(records[5]);
		clave_venta = Long.parseLong(records[6]);
	}

	public void parse(Text record) {
		parse(record.toString());
	}
}

class DataPoint {
	Date date;
	float value;
	static final SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd");

	public DataPoint(String dateStr, float value) throws ParseException {

		this.date = formater.parse(dateStr);
		this.value = value;
	}
}

class MaxFloatReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
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

class ParText {
	public Text text;

	public String x;
	public String y;

	public ParText(String x, String y) {
		this.x = x;
		this.y = y;

		this.text = new Text(this.x + "\t" + this.y);
	}

	public ParText(Text text) {
		this.text = text;

		String[] items = text.toString().split("\t");
		this.x = items[0];
		this.y = items[1];
	}
}

class Clave {
	public Text text;

	public String categoria;
	public String departemento;
	public String fecha;
	public long codigoProd;
	public long codigoLocal;

	private static final String NULL_S = "NULL";
	private static final String NULL_D = "2000-01-01";
	private static final short NULL_I = -1;

	public Clave(String categoria, String departamento, String fecha, long codigoProd, long codigoLocal) {
		this.categoria = categoria;
		this.departemento = departamento;
		this.fecha = fecha;
		this.codigoProd = codigoProd;
		this.codigoLocal = codigoLocal;

		this.text = new Text(this.categoria + "\t" + this.departemento + "\t" + this.fecha + "\t" + this.codigoProd
				+ "\t" + this.codigoLocal);
	}

	public Clave(String categoria, String departamento, String fecha) {
		this(categoria, departamento, fecha, NULL_I, NULL_I);
	}

	public Clave(String categoria, String fecha) {
		this(categoria, NULL_S, fecha, NULL_I, NULL_I);
	}

	public Clave(String categoria, String fecha, long codigoProd) {
		this(categoria, NULL_S, fecha, codigoProd, NULL_I);
	}

	public Clave(String categoria) {
		this(categoria, NULL_S, NULL_D, NULL_I, NULL_I);
	}

	public Clave(Text text) {
		this.text = text;

		String[] items = text.toString().split("\t");
		this.categoria = items[0];
		this.departemento = items[1];
		this.fecha = items[2];
		this.codigoProd = Integer.parseInt(items[3]);
		this.codigoLocal = Integer.parseInt(items[4]);
	}
}

class IdentityMapper<KEYIN, VALUEIN> extends Mapper<KEYIN, VALUEIN, KEYIN, VALUEIN> {
	public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}

class HdfsHashJoinMapper extends CacheHdfs.CMapper<LongWritable, Text, Text, Text> {
	private VentasParser ventasParser = new VentasParser();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			ventasParser.parse(value);

			long prod = ventasParser.clave_producto;
			String categoria = cacheHdfs.baseProductos.get(prod);

			long local = ventasParser.clave_local;
			String departamento = cacheHdfs.baseLocales.get(local);

			// FILTRADO DE CATEGORIAS
			if (categoria == null || categoria.equals("CENSURADO"))
				return;

			// FILTRADO DE DEPARTAMENTOS
			if (departamento == null || (!departamento.equals("MONTEVIDEO") && !departamento.equals("CANELONES")))
				return;

			// FILTRADO DE VENTAS (devoluciones, precios disparatados)
			if (ventasParser.cant_vta_original < 0 || ventasParser.precio_unitario < 10
					|| ventasParser.precio_unitario > 2000)
				return;

			// new LongWritable(ventasParser.clave_local)
			// new LongWritable(ventasParser.clave_producto)
			// new LongWritable(ventasParser.clave_venta)
			// new FloatWritable(ventasParser.precio_unitario)
			// new FloatWritable(ventasParser.cant_vta)
			// new FloatWritable(ventasParser.cant_vta_original)

			// ELECCION DE CLAVE
			context.write(new Clave(categoria, ventasParser.fecha.substring(0, 4), ventasParser.clave_producto).text,
					new ParText(ventasParser.fecha, Float.toString(ventasParser.precio_unitario)).text);

		} catch (NumberFormatException e) {
			System.err.println(e);
		}
	}
}

class BaseDriver extends Configured implements Tool {
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

public class MainDriver extends CacheHdfs.CDriver {
	@Override
	public void configureJob(Job job, int i) {
		job.setMapperClass(HdfsHashJoinMapper.class);
		// job.setCombinerClass(job_combine_class);
		job.setReducerClass(SegmentedRegressionReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}

}