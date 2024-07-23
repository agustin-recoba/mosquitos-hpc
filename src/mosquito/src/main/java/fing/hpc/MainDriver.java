package fing.hpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.collections.OrderedMap;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

class SegmentedRegression {

	public static class DataPoint {
		Date date;
		float value;

		public DataPoint(String dateStr, float value) throws ParseException {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			this.date = sdf.parse(dateStr);
			this.value = value;
		}
	}

	public static List<Date> detectChangepoints(Iterable<DataPoint> dataPoints) {
		List<DataPoint> data = new ArrayList<DataPoint>();
		for (DataPoint point : dataPoints) {
			data.add(point);
		}

		List<Date> changepoints = new ArrayList<Date>();
		int n = data.size();

		for (int i = 1; i < n - 1; i++) {
			List<DataPoint> leftSegment = data.subList(0, i + 1);
			List<DataPoint> rightSegment = data.subList(i + 1, n);

			double leftError = calculateSegmentError(leftSegment);
			double rightError = calculateSegmentError(rightSegment);

			double totalError = leftError + rightError;
			double currentError = calculateSegmentError(data);

			if (judgePoint(totalError, currentError)) {
				changepoints.add(data.get(i).date);
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
			return 0;

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

	class SegmentedRegressionReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			List<DataPoint> dataPoints = new ArrayList<>();
			for (Text t : values) {
				ParText vars = new ParText(t);
				try {
					dataPoints.add(new DataPoint(vars.x, Float.parseFloat(vars.y)));
				} catch (ParseException e) {
					continue;
				}
			}

			List<Date> changePoints = detectChangepoints(dataPoints);

			context.write(key, new Text(changePoints.toString()));
		}
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
	public int codigoProd;
	public int codigoLocal;

	private static final String NULL_S = "NULL";
	private static final String NULL_D = "2000-01-01";
	private static final int NULL_I = -1;

	public Clave(String categoria, String departamento, String fecha, int codigoProd, int codigoLocal) {
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

	public Clave(String categoria, String departamento, int codigoProd) {
		this(categoria, departamento, NULL_D, codigoProd, NULL_I);
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

class LectorCacheHdfsMapper<INKEY, INVAL, OUTKEY, OUTVAL> extends Mapper<INKEY, INVAL, OUTKEY, OUTVAL> {
	/*
	 * MAPPER QUE PUEDE ACCEDER A LAS BASES DE PRODUCTOS O LOCALES DESDE EL CACHE
	 * DISTRIBUIDO
	 */

	public HashMap<Long, String> baseLocales;
	public HashMap<Long, String> baseProductos;

	public LocalesParser localesParser = new LocalesParser();
	public ProductosParser productosParser = new ProductosParser();

	public void setup(Context context) throws IOException, InterruptedException {
		leerCacheHDFS(context);
	}

	public void leerCacheHDFS(Context context) throws IOException, InterruptedException {
		baseLocales = new HashMap<Long, String>();
		baseProductos = new HashMap<Long, String>();

		URI[] cacheFiles = context.getCacheFiles();

		if (cacheFiles != null && cacheFiles.length == 2) {
			for (URI cacheFile : cacheFiles) {
				boolean esLocales = cacheFile.getPath().contains("locales");
				try {
					String line = "";

					FileSystem fs = FileSystem.get(context.getConfiguration());
					Path getFilePath = new Path(cacheFile.toString());

					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

					while ((line = reader.readLine()) != null) {
						if (esLocales) {
							localesParser.parse(line);
							System.out.println(line);

							baseLocales.put(localesParser.clave, localesParser.departamento);
						} else {
							productosParser.parse(line);
							System.out.println(line);

							baseProductos.put(productosParser.clave, productosParser.categoria);
						}
					}
				} catch (NumberFormatException e) {
					System.err.println(e);
				} catch (Exception e) {
					throw new IOException("No se pudo leer el archivo de cache.");
				}
			}
		} else {
			throw new IOException("Archivo chache no se cargó.");
		}
	}

}

class LectorCacheHdfsReducer<INKEY, INVAL, OUTKEY, OUTVAL> extends Reducer<INKEY, INVAL, OUTKEY, OUTVAL> {
	/*
	 * REDUCER QUE PUEDE ACCEDER A LAS BASES DE PRODUCTOS O LOCALES DESDE EL CACHE
	 * DISTRIBUIDO
	 */

	public HashMap<Long, String> baseLocales;
	public HashMap<Long, String> baseProductos;

	public LocalesParser localesParser = new LocalesParser();
	public ProductosParser productosParser = new ProductosParser();

	public void setup(Context context) throws IOException, InterruptedException {
		leerCacheHDFS(context);
	}

	public void leerCacheHDFS(Context context) throws IOException, InterruptedException {
		baseLocales = new HashMap<Long, String>();
		baseProductos = new HashMap<Long, String>();

		URI[] cacheFiles = context.getCacheFiles();

		if (cacheFiles != null && cacheFiles.length == 2) {
			for (URI cacheFile : cacheFiles) {
				boolean esLocales = cacheFile.getPath().contains("locales");
				try {
					String line = "";

					FileSystem fs = FileSystem.get(context.getConfiguration());
					Path getFilePath = new Path(cacheFile.toString());

					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

					while ((line = reader.readLine()) != null) {
						if (esLocales) {
							localesParser.parse(line);
							System.out.println(line);

							baseLocales.put(localesParser.clave, localesParser.departamento);
						} else {
							productosParser.parse(line);
							System.out.println(line);

							baseProductos.put(productosParser.clave, productosParser.categoria);
						}
					}
				} catch (NumberFormatException e) {
					System.err.println(e);
				} catch (Exception e) {
					throw new IOException("No se pudo leer el archivo de cache.");
				}
			}
		} else {
			throw new IOException("Archivo chache no se cargó.");
		}
	}
}

class HdfsHashJoinMapper extends LectorCacheHdfsMapper<LongWritable, Text, Text, Text> {
	private VentasParser ventasParser = new VentasParser();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			ventasParser.parse(value);

			long prod = ventasParser.clave_producto;
			String categoria = baseProductos.get(prod);

			long local = ventasParser.clave_local;
			String departamento = baseLocales.get(local);

			// FILTRADO DE CATEGORIAS
			if (categoria == null || categoria.equals("CENSURADO"))
				return;

			// FILTRADO DE DEPARTAMENTOS
			if (departamento == null || (!departamento.equals("MONTEVIDEO") && !departamento.equals("CANELONES")))
				return;

			// new LongWritable(ventasParser.clave_local)
			// new LongWritable(ventasParser.clave_producto)
			// new LongWritable(ventasParser.clave_venta)
			// new FloatWritable(ventasParser.precio_unitario)
			// new FloatWritable(ventasParser.cant_vta)
			// new FloatWritable(ventasParser.cant_vta_original)

			// ELECCION DE CLAVE
			context.write(new Clave(categoria).text,
					new ParText(ventasParser.fecha, Float.toString(ventasParser.precio_unitario)).text);

		} catch (NumberFormatException e) {
			System.err.println(e);
		}
	}

}



public class MainDriver extends Configured implements Tool {
	static final Class<? extends Mapper> job_map_class = HdfsHashJoinMapper.class;
	static final Class<? extends Reducer> job_combine_class = null;
	static final Class<? extends Reducer> job_reduce_class = SegmentedRegression.SegmentedRegressionReducer.class;

	static final Class<? extends Writable> out_key_class = Text.class;
	static final Class<? extends Writable> out_value_class = Text.class;

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Uso: %s <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance(getConf(), "HPC - Mosquitos");
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(job_map_class);
		if (job_combine_class != null)
			job.setCombinerClass(job_combine_class);
		job.setReducerClass(job_reduce_class);

		job.setOutputKeyClass(out_key_class);
		job.setOutputValueClass(out_value_class);

		preRun(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	void preRun(Job job) {
		try {
			job.addCacheFile(new URI("hdfs://hadoop-master:9000/data/locales.csv"));
		} catch (Exception e) {
			System.out.println("Archivo de locales no se agregó al caché distribuido");
			System.exit(1);
		}

		try {
			job.addCacheFile(new URI("hdfs://hadoop-master:9000/data/productos.csv"));
		} catch (Exception e) {
			System.out.println("Archivo de productos no se agregó al caché distribuido");
			System.exit(1);
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MainDriver(), args);
		System.exit(exitCode);
	}

}