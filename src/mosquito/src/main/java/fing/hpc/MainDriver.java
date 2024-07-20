package fing.hpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
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

class HdfsHashJoinMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	private HashMap<Long, String> baseLocales;
	private HashMap<Long, String> baseProductos;

	private LocalesParser localesParser = new LocalesParser();
	private ProductosParser productosParser = new ProductosParser();
	private VentasParser ventasParser = new VentasParser();

	public static Text crearClave(String categoria, String departamento, String fecha) {
		return new Text(categoria + ";" + departamento + ";" + fecha);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			ventasParser.parse(value);

			long prod = ventasParser.clave_producto;
			String categoria = baseProductos.get(prod);

			long local = ventasParser.clave_local;
			String departamento = baseLocales.get(local);

			if (categoria == null || categoria.equals("CENSURADO"))
				return;

			if (departamento == null || (!departamento.equals("MONTEVIDEO") && !departamento.equals("CANELONES")))
				return;

			Text newKey = crearClave(categoria, departamento, ventasParser.fecha);

			// new LongWritable(ventasParser.clave_local)
			// new LongWritable(ventasParser.clave_producto)
			// new LongWritable(ventasParser.clave_venta)
			// new FloatWritable(ventasParser.precio_unitario)
			// new FloatWritable(ventasParser.cant_vta)
			// new FloatWritable(ventasParser.cant_vta_original)

			context.write(newKey, new FloatWritable(ventasParser.precio_unitario));

		} catch (NumberFormatException e) {
			System.err.println(e);
		}
	}

	public void setup(Context context) throws IOException, InterruptedException {
		leerCahceHDFS(context);
	}

	public void leerCahceHDFS(Context context) throws IOException, InterruptedException {
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

public class MainDriver extends Configured implements Tool {

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

		job.setMapperClass(HdfsHashJoinMapper.class);
		job.setCombinerClass(MaxFloatReducer.class);
		job.setReducerClass(MaxFloatReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

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

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MainDriver(), args);
		System.exit(exitCode);
	}

}