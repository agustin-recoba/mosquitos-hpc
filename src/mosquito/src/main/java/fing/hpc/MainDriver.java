package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

class DataPoint {
	Date date;
	double value;
	static final SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd");

	public DataPoint(String dateStr, double value) throws ParseException {

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

class ValuePair {
	public Text text;

	public String x;
	public String y;

	public ValuePair(String x, String y) {
		this.x = x;
		this.y = y;

		this.text = new Text(this.x + "\t" + this.y);
	}

	public ValuePair(Text text) {
		this.text = text;

		String[] items = text.toString().split("\t");
		this.x = items[0];
		this.y = items[1];
	}
}

class CustomKey {
	public Text text;

	public String categoria;
	public String departamento;
	public String fecha;
	public long codigoProd;
	public long codigoLocal;

	public static final String NULL_S = "NULL";
	public static final String NULL_D = "2000-01-01";
	public static final short NULL_I = -1;

	public CustomKey(String categoria, String departamento, String fecha, long codigoProd, long codigoLocal) {
		this.categoria = categoria;
		this.departamento = departamento;
		this.fecha = fecha;
		this.codigoProd = codigoProd;
		this.codigoLocal = codigoLocal;

		this.text = new Text(this.categoria + "\t" + this.departamento + "\t" + this.fecha + "\t" + this.codigoProd
				+ "\t" + this.codigoLocal);
	}

	public CustomKey(String categoria, String departamento, String fecha) {
		this(categoria, departamento, fecha, NULL_I, NULL_I);
	}

	public CustomKey(String categoria, String fecha) {
		this(categoria, NULL_S, fecha, NULL_I, NULL_I);
	}

	public CustomKey(String categoria, String fecha, long codigoProd) {
		this(categoria, NULL_S, fecha, codigoProd, NULL_I);
	}

	public CustomKey(String categoria) {
		this(categoria, NULL_S, NULL_D, NULL_I, NULL_I);
	}

	public CustomKey(Text text) {
		this.text = text;

		String[] items = text.toString().split("\t");
		this.categoria = items[0];
		this.departamento = items[1];
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

public class MainDriver extends CacheHdfs.CDriver {

	public MainDriver() {
		super();
		jobCount = 1;
	}

	@Override
	public void configureJob(Job job, int i) throws Exception {
		super.configureJob(job, i);
		
		job.setMapperClass(HdfsHashJoinMapper.class);
		// job.setCombinerClass(job_combine_class);
		job.setReducerClass(GroupByReducer.SumAll.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MainDriver(), args);
		System.exit(exitCode);
	}
}