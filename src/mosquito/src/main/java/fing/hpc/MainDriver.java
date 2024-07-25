package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;



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
	Parsers.Ventas ventasParser = new Parsers.Ventas();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			ventasParser.parse(value);

			String categoria = cacheHdfs.baseProductos.get(ventasParser.clave_producto);

			String departamento = cacheHdfs.baseLocales.get(ventasParser.clave_local);

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