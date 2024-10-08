package fing.hpc;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

class DataPoint {
	Date date;
	double value;
	static final SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd");

	public DataPoint(String dateStr, double value) throws ParseException {

		this.date = formater.parse(dateStr);
		this.value = value;
	}

	public DataPoint(Text v) throws ParseException {
		ValuePair vars = new ValuePair(v);
		this.date = formater.parse(vars.x);
		this.value = Float.parseFloat(vars.y);
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

	public CustomKey(String categoria, String fecha, long codigoProd, long codigoLocal) {
		this(categoria, NULL_S, fecha, codigoProd, codigoLocal);
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

class IdentityMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] items = value.toString().split("\t");
		context.write(new Text(items[0] + "\t" + items[1]), new Text(items[2]));
	}
}

class CatDepAnioHashJoinMapper extends HdfsHashJoinMapper {
	@Override
	public Text getResultKey() {
		return new CustomKey( // Cat, año y departamento
				cacheHdfs.baseProductos.get(ventasParser.clave_producto),
				cacheHdfs.baseLocales.get(ventasParser.clave_local),
				ventasParser.fecha.substring(0, 4)).text;
	}
}
